import pandas as pd
import numpy as np
from retrieval.bm25 import bm25_candidates, normalize_minmax


# =========================================================
# FIXED FINAL PARAMETERS
# =========================================================
TOP_N_CANDIDATES = 2000
TOP_K_RESULTS = 40
MIN_UMKM_RATIO = 0.4

ALPHA = 0.6
BETA = 0.15
GAMMA = 0.25
LAMBDA_UMKM = 0.1

POPULARITY_SOLD_WEIGHT = 0.6
POPULARITY_REVIEW_WEIGHT = 0.4
VALUE_RATING_WEIGHT = 0.7
VALUE_DISCOUNT_WEIGHT = 0.3


# =========================================================
# HYBRID SCORING
# =========================================================
def compute_balanced_hybrid(
    df_candidates,
    alpha=ALPHA,
    beta=BETA,
    gamma=GAMMA,
    lambda_umkm=LAMBDA_UMKM,
    popularity_sold_weight=POPULARITY_SOLD_WEIGHT,
    popularity_review_weight=POPULARITY_REVIEW_WEIGHT,
    value_rating_weight=VALUE_RATING_WEIGHT,
    value_discount_weight=VALUE_DISCOUNT_WEIGHT
):
    """
    Menghitung skor hybrid untuk kandidat hasil BM25.
    """

    # salin dataframe kandidat BM25 agar data asli tidak berubah
    df = df_candidates.copy()

    # =====================================================
    # --- Pastikan umkm_label konsisten ---
    # =====================================================
    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0
    })
    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)
    
    # =====================================================
    # --- Relevance ---
    # =====================================================
    df["bm25_norm"] = normalize_minmax(df["bm25_score"])

    # =====================================================
    # --- Popularity ---
    # =====================================================
    df["popularity_raw"] = (
        popularity_sold_weight * np.log1p(df["countSold"].fillna(0)) +
        popularity_review_weight * np.log1p(df["countReview"].fillna(0))
    )
    df["popularity_norm"] = normalize_minmax(df["popularity_raw"])

    # =====================================================
    # --- Value Score --- (awalnya Quality_score)
    # =====================================================
    df["rating_norm"] = normalize_minmax(df["ratingAverage"])
    df["discount_norm"] = normalize_minmax(df["discountPercentage"])
    df["value_score"] = (
        value_rating_weight * df["rating_norm"] +
        value_discount_weight * df["discount_norm"]
    )

    # =====================================================
    # --- Base Score ---
    # =====================================================
    df["base_score"] = (
        alpha * df["bm25_norm"] +
        beta * df["popularity_norm"] +
        gamma * df["value_score"]
    )

    # =====================================================
    # --- Fairness Adjustment ---
    # =====================================================
    df["final_score"] = df["base_score"] * (
        1 + lambda_umkm * df["umkm_label"]
    )

    # hasil diurutkan berdasarkan skor akhir tertinggi
    return df.sort_values("final_score", ascending=False).reset_index(drop=True)

# =========================================================
# UMKM-FIRST SELECTION + FAIRNESS SAFEGUARD
# =========================================================
def apply_umkm_priority_constraint(
    df_ranked,
    top_k=TOP_K_RESULTS,
    min_umkm_ratio=MIN_UMKM_RATIO
):
    """
    Aturan:
    1. Prioritaskan UMKM untuk mengisi top-K terlebih dahulu.
    2. Jika UMKM tidak cukup, isi sisa slot dengan NON_UMKM terbaik.
    3. min_umkm_ratio tetap dipertahankan sebagai pengaman minimum.
    """

    df = df_ranked.copy()
    df = df.sort_values("final_score", ascending=False).reset_index(drop=True)

    df_umkm = df[df["umkm_label"] == 1].copy()
    df_non_umkm = df[df["umkm_label"] == 0].copy()

    # target minimum UMKM berdasarkan rasio
    min_umkm_count = int(np.ceil(min_umkm_ratio * top_k))

    # ambil UMKM sebanyak mungkin sampai top_k
    selected_umkm = df_umkm.head(top_k).copy()
    umkm_count = len(selected_umkm)

    remaining_slots = top_k - umkm_count

    if remaining_slots > 0:
        selected_non_umkm = df_non_umkm.head(remaining_slots).copy()
        final_results = pd.concat(
            [selected_umkm, selected_non_umkm],
            ignore_index=True
        )
    else:
        final_results = selected_umkm.copy()

    # safety check:
    # jika jumlah UMKM di hasil akhir masih kurang dari minimum,
    # coba ambil UMKM tambahan dari luar hasil akhir dan tukar dengan non-UMKM terbawah
    current_umkm_count = int(final_results["umkm_label"].sum())

    if current_umkm_count < min_umkm_count:
        needed = min_umkm_count - current_umkm_count

        extra_umkm = df_umkm[
            ~df_umkm.index.isin(final_results.index)
        ].head(needed).copy()

        non_umkm_in_final = final_results[
            final_results["umkm_label"] == 0
        ].sort_values("final_score", ascending=True)

        replace_count = min(len(extra_umkm), len(non_umkm_in_final), needed)

        if replace_count > 0:
            to_remove = non_umkm_in_final.head(replace_count)
            to_add = extra_umkm.head(replace_count)

            final_results = final_results.drop(to_remove.index, errors="ignore")
            final_results = pd.concat([final_results, to_add], ignore_index=True)

    # urutan akhir:
    # UMKM di atas dulu, masing-masing tetap berdasarkan final_score tertinggi
    final_results["umkm_priority"] = final_results["umkm_label"]

    final_results = final_results.sort_values(
        by=["umkm_priority", "final_score"],
        ascending=[False, False]
    ).reset_index(drop=True)

    final_results["final_rank"] = np.arange(1, len(final_results) + 1)

    return final_results.drop(columns=["umkm_priority"])


# =========================================================
# Full Balanced Hybrid Pipeline
# =========================================================
def balanced_hybrid_search(
    query,
    top_n_candidates=TOP_N_CANDIDATES,
    top_k_results=TOP_K_RESULTS,
    min_umkm_ratio=MIN_UMKM_RATIO,
    alpha=ALPHA,
    beta=BETA,
    gamma=GAMMA,
    lambda_umkm=LAMBDA_UMKM,
    popularity_sold_weight=POPULARITY_SOLD_WEIGHT,
    popularity_review_weight=POPULARITY_REVIEW_WEIGHT,
    value_rating_weight=VALUE_RATING_WEIGHT,
    value_discount_weight=VALUE_DISCOUNT_WEIGHT
):
    """
    Pipeline:
    1. Ambil kandidat dari BM25
    2. Hitung skor hybrid
    3. Prioritaskan UMKM masuk top-K
    4. Jika UMKM tidak cukup, isi dengan NON_UMKM terbaik
    5. min_umkm_ratio tetap dipakai sebagai fairness safeguard
    """

    candidates = bm25_candidates(query, top_n=top_n_candidates)

    ranked = compute_balanced_hybrid(
        candidates,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        lambda_umkm=lambda_umkm,
        popularity_sold_weight=popularity_sold_weight,
        popularity_review_weight=popularity_review_weight,
        value_rating_weight=value_rating_weight,
        value_discount_weight=value_discount_weight
    )

    final_results = apply_umkm_priority_constraint(
        ranked,
        top_k=top_k_results,
        min_umkm_ratio=min_umkm_ratio
    )

    return final_results