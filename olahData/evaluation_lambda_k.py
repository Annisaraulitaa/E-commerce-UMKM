import numpy as np
import pandas as pd
import re
from retrieval.bm25 import bm25_search, bm25_candidates
from reranking.hybrid_rerank import balanced_hybrid_search


def relevant_mask(series, query, threshold=0.5):
    q_tokens = re.findall(r'\w+', query.lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k=20, threshold=0.5):
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return mask.mean()


def recall_at_k(df_topk, df_all, query, k=20, threshold=0.5):
    all_rel = relevant_mask(df_all["name_clean"], query, threshold)
    total_relevant = all_rel.sum()

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return topk_rel.sum() / total_relevant


def fairness_at_k(df, k=20):
    return df.head(k)["umkm_label"].mean()


def ndcg_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return dcg / idcg if idcg > 0 else 0


def exposure_disparity_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rank"] = np.arange(1, len(df_k) + 1)
    df_k["exposure"] = 1 / np.log2(df_k["rank"] + 1)

    umkm_exp = df_k[df_k["umkm_label"] == 1]["exposure"].mean()
    non_umkm_exp = df_k[df_k["umkm_label"] == 0]["exposure"].mean()

    if np.isnan(umkm_exp):
        umkm_exp = 0.0
    if np.isnan(non_umkm_exp):
        non_umkm_exp = 0.0

    return abs(umkm_exp - non_umkm_exp)


def prepare_label(df):
    df = df.copy()
    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0
    })
    df["umkm_label"] = pd.to_numeric(df["umkm_label"], errors="coerce").fillna(0).astype(int)
    return df


if __name__ == "__main__":

    test_queries = [
        "kopi khas daerah",
        "kopi instan sachet",
        "baju batik pria",
        "tas wanita kulit",
        "keripik singkong",
        "hiasan rumah handmade",
    ]

    lambda_values = [0.0, 0.1, 0.2, 0.3, 0.4]
    k_values = [10, 20, 30, 40, 50]

    all_results = []

    for q in test_queries:
        print(f"\n=== QUERY: {q} ===")

        candidate_pool = prepare_label(bm25_candidates(q, top_n=2000))

        for k in k_values:
            # BM25 baseline
            bm25_res = prepare_label(bm25_search(q, topk=k))

            all_results.append({
                "query": q,
                "method": "BM25",
                "lambda": None,
                "K": k,
                "Precision": precision_at_k(bm25_res, q, k),
                "Recall": recall_at_k(bm25_res, candidate_pool, q, k),
                "NDCG": ndcg_at_k(bm25_res, k),
                "Fairness": fairness_at_k(bm25_res, k),
                "ExposureDisparity": exposure_disparity_at_k(bm25_res, k),
            })

            # Hybrid multi-lambda
            for lam in lambda_values:
                hybrid_res = prepare_label(
                    balanced_hybrid_search(
                        query=q,
                        top_n_candidates=2000,
                        top_k_results=k,
                        min_umkm_ratio=0.0,
                        lambda_umkm=lam
                    )
                )

                all_results.append({
                    "query": q,
                    "method": "Hybrid",
                    "lambda": lam,
                    "K": k,
                    "Precision": precision_at_k(hybrid_res, q, k),
                    "Recall": recall_at_k(hybrid_res, candidate_pool, q, k),
                    "NDCG": ndcg_at_k(hybrid_res, k),
                    "Fairness": fairness_at_k(hybrid_res, k),
                    "ExposureDisparity": exposure_disparity_at_k(hybrid_res, k),
                })

    df_results = pd.DataFrame(all_results)
    df_results.to_csv("evaluation_lambda_k_results.csv", index=False)

    summary = (
        df_results[df_results["method"] == "Hybrid"]
        .groupby(["lambda", "K"])[["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]]
        .mean()
        .reset_index()
    )

    summary.to_csv("summary_lambda_k.csv", index=False)

    print("\n===== SUMMARY LAMBDA x K =====")
    print(summary)