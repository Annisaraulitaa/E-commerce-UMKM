# =========================================================
# HYBRID RE-RANKING FOR UMKM-AWARE RECOMMENDATION
# =========================================================

import pandas as pd
import numpy as np
from retrieval.bm25 import bm25_candidates, normalize_minmax


# =========================================================
# DEFAULT PARAMETERS 
# =========================================================
TOP_N_CANDIDATES = 2000
TOP_K_RESULTS = 60
MIN_UMKM_RATIO = 0.4

ALPHA = 0.50
BETA = 0.25
GAMMA = 0.25
LAMBDA_UMKM = 0.30

POPULARITY_SOLD_WEIGHT = 0.50
POPULARITY_REVIEW_WEIGHT = 0.25
POPULARITY_TOTAL_RATING_WEIGHT = 0.25
VALUE_RATING_WEIGHT = 0.70
VALUE_DISCOUNT_WEIGHT = 0.30


# =========================================================
# HELPER FUNCTIONS
# =========================================================
def normalize_umkm_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "umkm_label" not in df.columns:
        df["umkm_label"] = 0

    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1, "NON_UMKM": 0,
    })

    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"], 
        errors="coerce"
    ).fillna(0).astype(int)

    return df


def safe_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([0] * len(df), index=df.index)

    return pd.to_numeric(df[col], errors="coerce").fillna(0)


# =========================================================
# HYBRID SCORING
# =========================================================
def compute_balanced_hybrid(
    df_candidates: pd.DataFrame,
    alpha: float = ALPHA,
    beta: float = BETA,
    gamma: float = GAMMA,
    lambda_umkm: float = LAMBDA_UMKM,
    popularity_sold_weight: float = POPULARITY_SOLD_WEIGHT,
    popularity_review_weight: float = POPULARITY_REVIEW_WEIGHT,
    popularity_total_rating_weight: float = POPULARITY_TOTAL_RATING_WEIGHT,
    value_rating_weight: float = VALUE_RATING_WEIGHT,
    value_discount_weight: float = VALUE_DISCOUNT_WEIGHT
) -> pd.DataFrame:
    """
    Menghitung skor hybrid dari kandidat BM25.

    Komponen skor:
    - Relevance  : bm25_norm
    - Popularity : countSold, countReview, dan totalRating
    - Value      : ratingAverage dan discountPercentage
    - UMKM bonus : tambahan skor untuk produk UMKM
    """

    if df_candidates.empty:
        return pd.DataFrame()

    df = normalize_umkm_label(df_candidates)
    
    # =====================================================
    # 1. Relevance Score
    # =====================================================
    df["bm25_norm"] = normalize_minmax(df["bm25_score"])

    # =====================================================
    # 2. Popularity Score
    # =====================================================
    count_sold = safe_numeric(df, "countSold")
    count_review = safe_numeric(df, "countReview")
    total_rating = safe_numeric(df, "totalRating")

    df["countSold_norm"] = normalize_minmax(np.log1p(count_sold))
    df["countReview_norm"] = normalize_minmax(np.log1p(count_review))
    df["totalRating_norm"] = normalize_minmax(np.log1p(total_rating))

    df["popularity_score"] = (
        popularity_sold_weight * df["countSold_norm"] +
        popularity_review_weight * df["countReview_norm"] +
        popularity_total_rating_weight * df["totalRating_norm"]
    )

    # =====================================================
    # 3. Value Score
    # =====================================================
    rating = safe_numeric(df, "ratingAverage")
    discount = safe_numeric(df, "discountPercentage")

    df["rating_norm"] = normalize_minmax(rating)
    df["discount_norm"] = normalize_minmax(discount)

    df["value_score"] = (
        value_rating_weight * df["rating_norm"] +
        value_discount_weight * df["discount_norm"]
    )

    # =====================================================
    # --- Base Score ---
    # =====================================================
    df["base_score"] = (
        alpha * df["bm25_norm"] +
        beta * df["popularity_score"] +
        gamma * df["value_score"]
    )

    # =====================================================
    # --- Fairness Adjustment ---
    # =====================================================
    df["final_score"] = (
        df["base_score"] + lambda_umkm * df["umkm_label"]
    ).clip(0.0, 1.0)

    # hasil diurutkan berdasarkan skor akhir tertinggi
    return df.sort_values(
        "final_score", 
        ascending=False
    ).reset_index(drop=True)

# =========================================================
# UMKM-FIRST SELECTION + FAIRNESS SAFEGUARD
# =========================================================
def apply_umkm_priority_constraint(
    df_ranked: pd.DataFrame,
    top_k: int = TOP_K_RESULTS,
    min_umkm_ratio: float = MIN_UMKM_RATIO
) -> pd.DataFrame:
    """
    Aturan:
    1. Prioritaskan produk UMKM untuk masuk ke daftar rekomendasi Top-K.
    2. Jika produk UMKM tidak cukup, isi sisa slot dengan NON_UMKM terbaik.
    3. Setelah kandidat Top-K terbentuk, urutkan kembali berdasarkan final_score.
    """

    if df_ranked.empty:
        return pd.DataFrame()

    df = normalize_umkm_label(df_ranked)
    df = df.sort_values("final_score", ascending=False).reset_index(drop=True)

    # target minimum UMKM berdasarkan rasio
    min_umkm_count = int(np.ceil(min_umkm_ratio * top_k))

    df_umkm = df[df["umkm_label"] == 1].copy()
    df_non_umkm = df[df["umkm_label"] == 0].copy()

    selected_umkm = df_umkm.head(top_k).copy()
    remaining_slots = top_k - len(selected_umkm)

    if remaining_slots > 0:
        selected_non_umkm = df_non_umkm.head(remaining_slots).copy()
        final_results = pd.concat(
            [selected_umkm, selected_non_umkm],
            ignore_index=True
        )
    else:
        final_results = selected_umkm.copy()


    # Jika jumlah UMKM di hasil akhir masih kurang dari minimum,
    # coba ambil UMKM tambahan dari luar hasil akhir dan tukar dengan non-UMKM terbawah
    current_umkm_count = int(final_results["umkm_label"].sum())

    if current_umkm_count < min_umkm_count:
        needed = min_umkm_count - current_umkm_count

        selected_ids = set(final_results.index)

        extra_umkm = df_umkm[
            ~df_umkm.index.isin(selected_ids)
        ].head(needed).copy()

        non_umkm_in_final = final_results[
            final_results["umkm_label"] == 0
        ].sort_values("final_score", ascending=True)

        replace_count = min(
            needed,
            len(extra_umkm), 
            len(non_umkm_in_final)
        )

        if replace_count > 0:
            to_remove_idx = non_umkm_in_final.head(replace_count).index
            to_add = extra_umkm.head(replace_count)

            final_results = final_results.drop(to_remove_idx, errors="ignore")
            final_results = pd.concat([final_results, to_add], ignore_index=True)

    # Urutan akhir: ranking biasa berdasarkan final_score.
    final_results = final_results.sort_values(
        "final_score",
        ascending=False
    ).reset_index(drop=True)

    final_results["final_rank"] = np.arange(1, len(final_results) + 1)

    return final_results


# =========================================================
# Full Balanced Hybrid Pipeline
# =========================================================
def balanced_hybrid_search(
    query: str,
    top_n_candidates: int = TOP_N_CANDIDATES,
    top_k_results: int = TOP_K_RESULTS,
    min_umkm_ratio: float = MIN_UMKM_RATIO,
    alpha: float = ALPHA,
    beta: float = BETA,
    gamma: float = GAMMA,
    lambda_umkm: float = LAMBDA_UMKM,
    popularity_sold_weight: float = POPULARITY_SOLD_WEIGHT,
    popularity_review_weight: float = POPULARITY_REVIEW_WEIGHT,
    popularity_total_rating_weight: float = POPULARITY_TOTAL_RATING_WEIGHT,
    value_rating_weight: float = VALUE_RATING_WEIGHT,
    value_discount_weight: float = VALUE_DISCOUNT_WEIGHT
):
    """
    Pipeline:
    1. Ambil kandidat dari BM25
    2. Hitung skor hybrid
    3. Prioritaskan UMKM masuk top-K
    4. Menghasilkan final recommendation list
    """

    query = str(query).strip()

    if not query:
        return pd.DataFrame()
    
    candidates = bm25_candidates(query, top_n=top_n_candidates)

    if candidates.empty:
        return pd.DataFrame()

    ranked = compute_balanced_hybrid(
        candidates,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        lambda_umkm=lambda_umkm,
        popularity_sold_weight=popularity_sold_weight,
        popularity_review_weight=popularity_review_weight,
        popularity_total_rating_weight=popularity_total_rating_weight,
        value_rating_weight=value_rating_weight,
        value_discount_weight=value_discount_weight
    )

    if ranked.empty:
        return pd.DataFrame()

    final_results = apply_umkm_priority_constraint(
        ranked,
        top_k=top_k_results,
        min_umkm_ratio=min_umkm_ratio
    )

    return final_results


# =========================================================
# MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    print("Hybrid re-ranking siap digunakan.")

    q = input("Masukkan query: ").strip()

    results = balanced_hybrid_search(
        query=q,
        top_n_candidates=TOP_N_CANDIDATES,
        top_k_results=TOP_K_RESULTS,
        min_umkm_ratio=MIN_UMKM_RATIO,
        alpha=ALPHA,
        beta=BETA,
        gamma=GAMMA,
        lambda_umkm=LAMBDA_UMKM,
        popularity_sold_weight=POPULARITY_SOLD_WEIGHT,
        popularity_review_weight=POPULARITY_REVIEW_WEIGHT,
        popularity_total_rating_weight=POPULARITY_TOTAL_RATING_WEIGHT,
        value_rating_weight=VALUE_RATING_WEIGHT,
        value_discount_weight=VALUE_DISCOUNT_WEIGHT
    )

    print(f"\n=== HASIL HYBRID TOP-{TOP_K_RESULTS} ===")
    print(results.to_string(index=False))

    results.to_csv(
        "hybrid_rerank_results.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print("\nDisimpan: hybrid_rerank_results.csv")