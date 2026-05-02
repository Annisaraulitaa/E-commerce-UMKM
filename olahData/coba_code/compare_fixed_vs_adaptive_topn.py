# Dipakai untuk eksperimen membandingkan fixed top-N vs adaptive top-N dalam tahap kandidat BM25 sebelum reranking hybrid.
# Hasil eksperimen ini akan membantu menentukan apakah kita tetap pakai fixed top-N atau beralih ke adaptive top-N yang lebih fleksibel.


import numpy as np
import pandas as pd
import re

from retrieval.bm25_mix import (
    bm25_search,
    bm25_candidates_fixed,
    bm25_candidates_adaptive,
)
from reranking.hybrid_rerank import compute_balanced_hybrid, apply_umkm_priority_constraint


# =========================================================
# 1. METRIC FUNCTIONS
# =========================================================
def relevant_mask(series, query, threshold=0.5):
    q_tokens = re.findall(r"\w+", str(query).lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0.0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k=40, threshold=0.5):
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return float(mask.mean())


def recall_at_k(df_topk, df_pool, query, k=40, threshold=0.5):
    all_rel = relevant_mask(df_pool["name_clean"], query, threshold)
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return float(topk_rel.sum() / total_relevant)


def f1_at_k(precision, recall):
    if (precision + recall) == 0:
        return 0.0
    return 2 * precision * recall / (precision + recall)


def fairness_at_k(df, k=40):
    return float(df.head(k)["umkm_label"].mean())


def ndcg_at_k(df, k=40):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return float(dcg / idcg) if idcg > 0 else 0.0


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


# =========================================================
# 2. CONFIG
# =========================================================
TEST_QUERIES = [
    "kopi khas daerah",
    "kopi instan sachet",
    "baju batik pria",
    "tas wanita kulit",
    "keripik singkong",
    "hiasan rumah handmade",
]

TOP_K = 40
MIN_UMKM_RATIO = 0.4

# mode eksperimen
FIXED_TOPN = 2000
ADAPTIVE_CAP_1 = 2000
ADAPTIVE_CAP_2 = 5000

# parameter hybrid final
ALPHA = 0.6
BETA = 0.15
GAMMA = 0.25
LAMBDA_UMKM = 0.1

POPULARITY_SOLD_WEIGHT = 0.7
POPULARITY_REVIEW_WEIGHT = 0.3
VALUE_RATING_WEIGHT = 0.7
VALUE_DISCOUNT_WEIGHT = 0.3


# =========================================================
# 3. HYBRID FROM CANDIDATE POOL
# =========================================================
def hybrid_from_candidates(df_candidates):
    ranked = compute_balanced_hybrid(
        df_candidates,
        alpha=ALPHA,
        beta=BETA,
        gamma=GAMMA,
        lambda_umkm=LAMBDA_UMKM,
        popularity_sold_weight=POPULARITY_SOLD_WEIGHT,
        popularity_review_weight=POPULARITY_REVIEW_WEIGHT,
        value_rating_weight=VALUE_RATING_WEIGHT,
        value_discount_weight=VALUE_DISCOUNT_WEIGHT
    )

    final_results = apply_umkm_priority_constraint(
        ranked,
        top_k=TOP_K,
        min_umkm_ratio=MIN_UMKM_RATIO
    )

    return final_results


# =========================================================
# 4. EVALUATE ONE RESULT
# =========================================================
def evaluate_result(df_result, df_pool, query, k=40):
    df_result = prepare_label(df_result)
    df_pool = prepare_label(df_pool)

    precision = precision_at_k(df_result, query, k=k)
    recall = recall_at_k(df_result, df_pool, query, k=k)
    f1 = f1_at_k(precision, recall)

    return {
        "Precision@40": precision,
        "Recall@40": recall,
        "F1@40": f1,
        "NDCG@40": ndcg_at_k(df_result, k=k),
        "Fairness@40": fairness_at_k(df_result, k=k),
    }


# =========================================================
# 5. ADD ONE MODE RESULT
# =========================================================
def append_mode_result(rows, query, method_name, df_result, df_pool, pool_size_after, pool_size_before=None):
    metrics = evaluate_result(df_result, df_pool, query, k=TOP_K)
    metrics["query"] = query
    metrics["method"] = method_name
    metrics["candidate_pool_size_after_cap"] = pool_size_after
    metrics["candidate_pool_size_before_cap"] = pool_size_before
    rows.append(metrics)


# =========================================================
# 6. RUN EXPERIMENT
# =========================================================
def run_experiment():
    rows = []

    for q in TEST_QUERIES:
        print(f"\n=== QUERY: {q} ===")

        # baseline BM25 top-K
        bm25_result = prepare_label(
            bm25_search(q, topk=TOP_K, use_term_filter=True)
        )

        # ---------------------------------------------
        # Fixed TopN = 2000
        # ---------------------------------------------
        fixed_candidates = prepare_label(
            bm25_candidates_fixed(q, top_n=FIXED_TOPN)
        )
        fixed_hybrid = prepare_label(
            hybrid_from_candidates(fixed_candidates)
        )

        # ---------------------------------------------
        # Adaptive TopN cap = 2000
        # ---------------------------------------------
        adaptive_2000_candidates = prepare_label(
            bm25_candidates_adaptive(q, top_n_max=ADAPTIVE_CAP_1)
        )
        adaptive_2000_hybrid = prepare_label(
            hybrid_from_candidates(adaptive_2000_candidates)
        )

        adaptive_2000_before_cap = None
        if (
            not adaptive_2000_candidates.empty and
            "adaptive_pool_size_before_cap" in adaptive_2000_candidates.columns
        ):
            adaptive_2000_before_cap = int(
                adaptive_2000_candidates["adaptive_pool_size_before_cap"].iloc[0]
            )

        # ---------------------------------------------
        # Adaptive TopN cap = 5000
        # ---------------------------------------------
        adaptive_5000_candidates = prepare_label(
            bm25_candidates_adaptive(q, top_n_max=ADAPTIVE_CAP_2)
        )
        adaptive_5000_hybrid = prepare_label(
            hybrid_from_candidates(adaptive_5000_candidates)
        )

        adaptive_5000_before_cap = None
        if (
            not adaptive_5000_candidates.empty and
            "adaptive_pool_size_before_cap" in adaptive_5000_candidates.columns
        ):
            adaptive_5000_before_cap = int(
                adaptive_5000_candidates["adaptive_pool_size_before_cap"].iloc[0]
            )

        # tampilkan ukuran pool agar mudah dicek
        print(f"FixedTopN_2000 size                 : {len(fixed_candidates)}")
        print(f"AdaptiveTopN_cap2000 after cap      : {len(adaptive_2000_candidates)}")
        print(f"AdaptiveTopN_cap2000 before cap     : {adaptive_2000_before_cap}")
        print(f"AdaptiveTopN_cap5000 after cap      : {len(adaptive_5000_candidates)}")
        print(f"AdaptiveTopN_cap5000 before cap     : {adaptive_5000_before_cap}")

        # BM25 baseline
        append_mode_result(
            rows=rows,
            query=q,
            method_name="BM25",
            df_result=bm25_result,
            df_pool=fixed_candidates,
            pool_size_after=None,
            pool_size_before=None
        )

        # Hybrid fixed
        append_mode_result(
            rows=rows,
            query=q,
            method_name="Hybrid_FixedTopN_2000",
            df_result=fixed_hybrid,
            df_pool=fixed_candidates,
            pool_size_after=len(fixed_candidates),
            pool_size_before=len(fixed_candidates)
        )

        # Hybrid adaptive cap 2000
        append_mode_result(
            rows=rows,
            query=q,
            method_name="Hybrid_AdaptiveTopN_cap2000",
            df_result=adaptive_2000_hybrid,
            df_pool=adaptive_2000_candidates,
            pool_size_after=len(adaptive_2000_candidates),
            pool_size_before=adaptive_2000_before_cap
        )

        # Hybrid adaptive cap 5000
        append_mode_result(
            rows=rows,
            query=q,
            method_name="Hybrid_AdaptiveTopN_cap5000",
            df_result=adaptive_5000_hybrid,
            df_pool=adaptive_5000_candidates,
            pool_size_after=len(adaptive_5000_candidates),
            pool_size_before=adaptive_5000_before_cap
        )

    return pd.DataFrame(rows)


# =========================================================
# 7. MAIN
# =========================================================
if __name__ == "__main__":
    df_result = run_experiment()

    print("\n===== HASIL PER QUERY =====")
    print(df_result)

    df_result.to_csv("experiment_compare_topn_modes.csv", index=False)
    print("\nDisimpan: experiment_compare_topn_modes.csv")

    summary = (
        df_result
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

    print("\n===== RINGKASAN RATA-RATA =====")
    print(summary)

    summary.to_csv("experiment_compare_topn_modes_summary.csv", index=False)
    print("\nDisimpan: experiment_compare_topn_modes_summary.csv")