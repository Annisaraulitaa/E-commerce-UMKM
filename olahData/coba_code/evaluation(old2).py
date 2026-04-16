import numpy as np
import pandas as pd
import re
from retrieval.bm25 import bm25_candidates, bm25_search
from reranking.hybrid_rerank import balanced_hybrid_search


# =========================================================
# METRIC FUNCTIONS
# =========================================================

def precision_at_k(df, query, k=20, threshold=0.5):
    df_k = df.head(k)
    q_tokens = re.findall(r'\w+', query.lower())

    def relevant_score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0
    
    relevant = df_k["name_clean"].apply(relevant_score) >= threshold
    return relevant.mean()


def recall_at_k(df_topk, df_all, query, k=20, threshold=0.5):
    q_tokens = re.findall(r'\w+', query.lower())

    def relevant_score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    # seluruh item relevan di kumpulan kandidat
    all_rel = df_all["name_clean"].apply(relevant_score) >= threshold
    total_relevant = all_rel.sum()

    if total_relevant == 0:
        return 0.0

    # item relevan yang berhasil masuk top-K
    topk_rel = df_topk.head(k)["name_clean"].apply(relevant_score) >= threshold
    retrieved_relevant = topk_rel.sum()

    return retrieved_relevant / total_relevant


def fairness_at_k(df, k=20):
    df_k = df.head(k)
    return df_k["umkm_label"].mean()


def ndcg_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))

    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return dcg / idcg if idcg > 0 else 0


def prepare_label(df):
    df = df.copy()
    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0
    })
    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)
    return df


def evaluate_single_result(df_result, query, k=20):
    df_result = prepare_label(df_result)

    return {
        "Precision@20": precision_at_k(df_result, query, k, threshold=0.5),
        "NDCG@20": ndcg_at_k(df_result, k),
        "Fairness@20": fairness_at_k(df_result, k),
    }


# =========================================================
# EXPERIMENT RUNNER
# =========================================================

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

    all_results = []

    for q in test_queries:
        print(f"\n=== QUERY: {q} ===")

        candidate_pool = bm25_candidates(q, top_n=2000)
        candidate_pool = prepare_label(candidate_pool)

        # BM25 baseline
        bm25_results = bm25_search(q, topk=20)
        bm25_results = prepare_label(bm25_results)

        bm25_metrics = evaluate_single_result(bm25_results, q, k=20)
        bm25_metrics["Recall@20"] = recall_at_k(
            bm25_results,
            candidate_pool,
            q,
            k=20,
            threshold=0.5
        )

        bm25_metrics["query"] = q
        bm25_metrics["method"] = "BM25"
        bm25_metrics["lambda"] = None
        all_results.append(bm25_metrics)

        # Hybrid multi-lambda
        for lam in lambda_values:
            hybrid_results = balanced_hybrid_search(
                query=q,
                top_n_candidates=2000,
                top_k_results=20,
                min_umkm_ratio=0.0,   #sebelumnya =0.4
                lambda_umkm=lam
            )

            hybrid_results = prepare_label(hybrid_results)

            metrics = evaluate_single_result(hybrid_results, q, k=20)
            metrics["Recall@20"] = recall_at_k(
                hybrid_results,
                candidate_pool,
                q,
                k=20,
                threshold=0.5
            )

            metrics["query"] = q
            metrics["method"] = "Hybrid"
            metrics["lambda"] = lam
            all_results.append(metrics)

    df_eval = pd.DataFrame(all_results)

    print("\n===== HASIL EKSPERIMEN =====")
    print(df_eval)

    df_eval.to_csv("evaluation_results_multi_lambda.csv", index=False)
    print("\nDisimpan: evaluation_results_multi_lambda.csv")

    # Ringkasan rata-rata per lambda
    df_hybrid_summary = (
        df_eval[df_eval["method"] == "Hybrid"]
        .groupby("lambda")[["Precision@20", "Recall@20", "NDCG@20", "Fairness@20"]]
        .mean()
        .reset_index()
    )

    print("\n===== RATA-RATA HYBRID PER LAMBDA =====")
    print(df_hybrid_summary)

    df_hybrid_summary.to_csv("summary_multi_lambda.csv", index=False)
    print("\nDisimpan: summary_multi_lambda.csv")