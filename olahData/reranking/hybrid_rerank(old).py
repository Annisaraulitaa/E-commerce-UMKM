from retrieval.bm25 import bm25_candidates, normalize_minmax
import pandas as pd
import numpy as np

# =========================================================
# Hybrid Scoring
# =========================================================
def compute_balanced_hybrid(
    df_candidates,
    alpha=0.6,
    beta=0.15,
    gamma=0.25,
    lambda_umkm=0.1,
    popularity_sold_weight=0.6,
    popularity_review_weight=0.4,
    value_rating_weight=0.7,
    value_discount_weight=0.3
):
    """
    Menghitung skor hybrid untuk kandidat hasil BM25.
    Komponen skor:
    - Relevance    : bm25_norm
    - Popularity   : kombinasi countSold dan countReview
    - Value Score  : kombinasi ratingAverage dan discountPercentage
    - Fairness     : boost untuk produk UMKM melalui lambda_umkm
    """

    # salin dataframe kandidat BM25 agar data asli tidak berubah
    df = df_candidates.copy()

    # pastikan label UMKM dikonversi ke numeric
    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0
    })

    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)
    
    # --- Relevance ---
    df["bm25_norm"] = normalize_minmax(df["bm25_score"])

    # --- Popularity ---
    df["popularity_raw"] = (
        popularity_sold_weight * np.log1p(df["countSold"].fillna(0)) +
        popularity_review_weight * np.log1p(df["countReview"].fillna(0))
    )
    df["popularity_norm"] = normalize_minmax(df["popularity_raw"])

    # --- Value --- (awalnya namanya Quality_score)
    df["rating_norm"] = normalize_minmax(df["ratingAverage"])
    df["discount_norm"] = normalize_minmax(df["discountPercentage"])

    df["value_score"] = (
        value_rating_weight * df["rating_norm"] +
        value_discount_weight * df["discount_norm"]
    )

    # --- Base Score ---
    df["base_score"] = (
        alpha * df["bm25_norm"] +
        beta * df["popularity_norm"] +
        gamma * df["value_score"]
    )

    # --- Fairness Adjustment (Proportional) ---
    df["final_score"] = df["base_score"] * (
        1 + lambda_umkm * df["umkm_label"]
    )

    # hasil diurutkan berdasarkan skor akhir tertinggi
    return df.sort_values("final_score", ascending=False)


# =========================================================
# Fairness Constraint (Guarantee 40%)
# =========================================================
def apply_fairness_constraint(
    df_ranked,
    top_k=20,
    min_umkm_ratio=0.4
):
    
    # salin hasil ranking agar aman
    df = df_ranked.copy()

    # hitung proporsi UMKM di Top-K saat ini
    top_results = df.head(top_k).copy()
    current_ratio = top_results["umkm_label"].mean()

    # jika proporsi UMKM sudah memenuhi target, langsung kembalikan
    if current_ratio >= min_umkm_ratio:
        return top_results

    # Jumlah UMKM tambahan yang dibutuhkan
    needed_umkm = int(min_umkm_ratio * top_k) - int(top_results["umkm_label"].sum())

    if needed_umkm <= 0:
        return top_results.sort_values("final_score", ascending=False)

    # UMKM terbaik di luar Top-K
    additional_umkm = df[
        (df["umkm_label"] == 1) &
        (~df.index.isin(top_results.index))
    ].head(needed_umkm) # ambil UMKM terbaik dari luar Top-K sebanyak kebutuhan

    # Non-UMKM terendah di Top-K
    non_umkm_to_remove = top_results[
        top_results["umkm_label"] == 0
    ].tail(needed_umkm) # buang non-UMKM dengan skor paling rendah di Top-K

    # Jika tidak cukup UMKM pengganti, pakai sebanyak yang tersedia
    replace_count = min(len(additional_umkm), len(non_umkm_to_remove))

    if replace_count == 0:
        return top_results.sort_values("final_score", ascending=False)

    additional_umkm = additional_umkm.head(replace_count)
    non_umkm_to_remove = non_umkm_to_remove.tail(replace_count)

    # tukar non-UMKM terbawah dengan UMKM terbaik dari luar Top-K
    top_results = top_results.drop(non_umkm_to_remove.index)
    top_results = pd.concat([top_results, additional_umkm])

    # urutkan kembali berdasarkan final_score
    return top_results.sort_values("final_score", ascending=False)


# =========================================================
# Full Balanced Hybrid Pipeline
# =========================================================
def balanced_hybrid_search(
    query,
    top_n_candidates=2000,
    top_k_results=20,
    min_umkm_ratio=0.4,
    lambda_umkm=0.1,
    alpha=0.6,
    beta=0.15,
    gamma=0.25,
    popularity_sold_weight=0.6,
    popularity_review_weight=0.4,
    value_rating_weight=0.7,
    value_discount_weight=0.3
):
    """
    Pipeline lengkap hybrid ranking:
    1. Ambil kandidat dari BM25
    2. Hitung skor hybrid
    3. Terapkan fairness constraint
    """

    # BM25 Candidate Retrieval (top N ±2% dataset)
    candidates = bm25_candidates(query, top_n=top_n_candidates)

    # Balanced Hybrid Scoring
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

    # Fairness Guarantee
    final_results = apply_fairness_constraint(
        ranked,
        top_k=top_k_results,
        min_umkm_ratio=min_umkm_ratio
    )

    return final_results