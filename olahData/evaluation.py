import numpy as np
import pandas as pd
import re
from retrieval.bm25 import bm25_candidates, bm25_search
from reranking.hybrid_rerank import balanced_hybrid_search


# =========================================================
# FIXED FINAL PARAMETERS
# =========================================================
TOP_K = 40
TOP_N_CANDIDATES = 2000
MIN_UMKM_RATIO = 0.4

ALPHA = 0.6
BETA = 0.15
GAMMA = 0.25
LAMBDA_UMKM = 0.1

POPULARITY_SOLD_WEIGHT = 0.7
POPULARITY_REVIEW_WEIGHT = 0.3
VALUE_RATING_WEIGHT = 0.7
VALUE_DISCOUNT_WEIGHT = 0.3


# =========================================================
# METRIC FUNCTIONS
# =========================================================
def precision_at_k(df, query, k=40, threshold=0.5):
    df_k = df.head(k)
    q_tokens = re.findall(r"\w+", query.lower())

    def relevant_score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    relevant = df_k["name_clean"].apply(relevant_score) >= threshold
    return float(relevant.mean())


def recall_at_k(df_topk, df_all, query, k=40, threshold=0.5):
    q_tokens = re.findall(r"\w+", query.lower())

    def relevant_score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    # seluruh item relevan di kumpulan kandidat
    all_rel = df_all["name_clean"].apply(relevant_score) >= threshold
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    # item relevan yang berhasil masuk top-K
    topk_rel = df_topk.head(k)["name_clean"].apply(relevant_score) >= threshold
    retrieved_relevant = int(topk_rel.sum())

    return retrieved_relevant / total_relevant


def fairness_at_k(df, k=40):
    df_k = df.head(k)
    return float(df_k["umkm_label"].mean())


def ndcg_at_k(df, k=40):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return float(dcg / idcg) if idcg > 0 else 0.0


def f1_at_k(precision, recall):
    if (precision + recall) == 0:
        return 0.0
    return 2 * (precision * recall) / (precision + recall)


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


def evaluate_result(df_result, candidate_pool, query, k=40):
    df_result = prepare_label(df_result)
    candidate_pool = prepare_label(candidate_pool)

    precision = precision_at_k(df_result, query, k=k, threshold=0.5)
    recall = recall_at_k(df_result, candidate_pool, query, k=k, threshold=0.5)

    return {
        "Precision@40": precision,
        "Recall@40": recall,
        "F1@40": f1_at_k(precision, recall),
        "NDCG@40": ndcg_at_k(df_result, k=k),
        "Fairness@40": fairness_at_k(df_result, k=k),
    }


# =========================================================
# FINAL EVALUATION
# =========================================================
if __name__ == "__main__":

    test_queries = [
        #"kopi khas daerah",
        #"kopi instan sachet",
        #"baju batik pria",
        "tas wanita kulit",
        #"keripik singkong",
        #"hiasan rumah handmade",
    ]

    all_results = []

    for q in test_queries:
        print(f"\n=== QUERY: {q} ===")

        candidate_pool = bm25_candidates(q, top_n=TOP_N_CANDIDATES)

        # ----------------------------------------------------
        # BM25 BASELINE (pure relevance)
        # ----------------------------------------------------
        bm25_results = bm25_search(q, topk=TOP_K)
        bm25_metrics = evaluate_result(bm25_results, candidate_pool, q, k=TOP_K)
        bm25_metrics["query"] = q
        bm25_metrics["method"] = "BM25"
        all_results.append(bm25_metrics)

        # ----------------------------------------------------
        # HYBRID FINAL (UMKM-first logic)
        # ----------------------------------------------------
        hybrid_results = balanced_hybrid_search(
            query=q,
            top_n_candidates=TOP_N_CANDIDATES,
            top_k_results=TOP_K,
            min_umkm_ratio=MIN_UMKM_RATIO,
            lambda_umkm=LAMBDA_UMKM,
            alpha=ALPHA,
            beta=BETA,
            gamma=GAMMA,
            popularity_sold_weight=POPULARITY_SOLD_WEIGHT,
            popularity_review_weight=POPULARITY_REVIEW_WEIGHT,
            value_rating_weight=VALUE_RATING_WEIGHT,
            value_discount_weight=VALUE_DISCOUNT_WEIGHT
        )

        hybrid_metrics = evaluate_result(hybrid_results, candidate_pool, q, k=TOP_K)
        hybrid_metrics["query"] = q
        hybrid_metrics["method"] = "Hybrid_Final"
        all_results.append(hybrid_metrics)

    df_eval = pd.DataFrame(all_results)

    print("\n===== HASIL EVALUASI FINAL =====")
    print(df_eval)

    df_eval.to_csv("evaluation_results_final_top40.csv", index=False)
    print("\nDisimpan: evaluation_results_final_top40.csv")

    summary = (
        df_eval
        .groupby("method")[[
            "Precision@40",
            "Recall@40",
            "F1@40",
            "NDCG@40",
            "Fairness@40"
        ]]
        .mean()
        .reset_index()
    )

    print("\n===== RINGKASAN RATA-RATA FINAL =====")
    print(summary)

    summary.to_csv("summary_final_top40.csv", index=False)
    print("\nDisimpan: summary_final_top40.csv")