from retrieval.bm25 import bm25_candidates, normalize_minmax
import pandas as pd
import numpy as np

# =========================================================
# Hybrid Scoring
# =========================================================
def compute_balanced_hybrid(
    df_candidates,
    alpha=0.4,
    beta=0.15,
    gamma=0.15,
    lambda_umkm=0.3
):
    df = df_candidates.copy()

    # !Pastikan umkm_label aman!
    df["umkm_label"] = df["umkm_label"].fillna(0).astype(int)

    # --- Relevance ---
    df["bm25_norm"] = normalize_minmax(df["bm25_score"])

    # --- Popularity ---
    df["popularity_raw"] = (
        0.6 * np.log1p(df["countSold"].fillna(0)) +
        0.4 * np.log1p(df["countReview"].fillna(0))
    )
    df["popularity_norm"] = normalize_minmax(df["popularity_raw"])

    # --- Quality ---
    df["rating_norm"] = normalize_minmax(df["ratingAverage"])
    df["discount_norm"] = normalize_minmax(df["discountPercentage"])

    df["quality_score"] = (
        0.7 * df["rating_norm"] +
        0.3 * df["discount_norm"]
    )

    # --- Base Score ---
    df["base_score"] = (
        alpha * df["bm25_norm"] +
        beta * df["popularity_norm"] +
        gamma * df["quality_score"]
    )

    # --- Fairness Adjustment (Proportional) ---
    df["final_score"] = df["base_score"] * (
        1 + lambda_umkm * df["umkm_label"]
    )

    return df.sort_values("final_score", ascending=False)


# =========================================================
# Fairness Constraint (Guarantee 40%)
# =========================================================
def apply_fairness_constraint(
    df_ranked,
    top_k=20,
    min_umkm_ratio=0.4
):
    df = df_ranked.copy()

    top_results = df.head(top_k).copy()
    current_ratio = top_results["umkm_label"].mean()

    if current_ratio >= min_umkm_ratio:
        return top_results

    needed_umkm = int(min_umkm_ratio * top_k) - int(
        top_results["umkm_label"].sum()
    )

    # UMKM terbaik di luar Top-K
    additional_umkm = df[
        (df["umkm_label"] == 1) &
        (~df.index.isin(top_results.index))
    ].head(needed_umkm)

    # Non-UMKM terendah di Top-K
    non_umkm_to_remove = top_results[
        top_results["umkm_label"] == 0
    ].tail(needed_umkm)

    top_results = top_results.drop(non_umkm_to_remove.index)
    top_results = pd.concat([top_results, additional_umkm])

    return top_results.sort_values("final_score", ascending=False)


# =========================================================
# Full Balanced Hybrid Pipeline
# =========================================================
from retrieval.bm25 import bm25_candidates

def balanced_hybrid_search(
    query,
    top_n_candidates=2000,
    top_k_results=20,
    min_umkm_ratio=0.4
):
    # BM25 Candidate Retrieval
    candidates = bm25_candidates(query, top_n=top_n_candidates)

    # Balanced Hybrid Scoring
    ranked = compute_balanced_hybrid(candidates)

    # Fairness Guarantee
    final_results = apply_fairness_constraint(
        ranked,
        top_k=top_k_results,
        min_umkm_ratio=min_umkm_ratio
    )

    return final_results