# =========================================================
# SHOW DETAIL METRIC CALCULATION FOR ONE QUERY AND ONE K
# =========================================================
# Tujuan:
# - Menampilkan Top-K produk untuk satu query
# - Menampilkan manual_relevance tiap produk
# - Menghitung Precision@K, Recall@K, F1-score, dan NDCG@K
# - Mengambil konfigurasi terbaik dari file summary
# =========================================================

import numpy as np
import pandas as pd

from retrieval.bm25 import bm25_candidates
from reranking.hybrid_rerank import compute_balanced_hybrid, apply_umkm_priority_constraint

from tuning_manual_ground_truth import (
    TOP_N_CANDIDATES,
    MIN_UMKM_RATIO,
    load_manual_labels,
    add_manual_relevance,
)

from metrics import (
    precision_at_k,
    recall_at_k,
    f1_score,
    ndcg_at_k,
)

from utils import get_col


# =========================================================
# CONFIG
# =========================================================

SUMMARY_FILE = "manual_tuning_summary_relevance_only.csv"


# =========================================================
# GET BEST CONFIG
# =========================================================

def get_best_config_for_metric(summary_file, metric="Precision", k=20):
    df_summary = pd.read_csv(
        summary_file,
        sep=";",
        encoding="utf-8-sig"
    )

    df_k = df_summary[df_summary["K"] == k].copy()

    if df_k.empty:
        raise ValueError(
            f"Tidak ada data untuk K={k} pada file {summary_file}"
        )

    best_row = (
        df_k
        .sort_values(
            by=[metric, "NDCG", "Recall", "F1_score"],
            ascending=[False, False, False, False]
        )
        .iloc[0]
    )

    print("\n=================================================")
    print(f"KONFIGURASI TERBAIK BERDASARKAN {metric}@{k}")
    print("=================================================")
    print(best_row.to_string())

    return best_row


# =========================================================
# NDCG DETAIL
# =========================================================

def calculate_dcg_idcg_detail(labels, total_relevant, k):
    labels_array = np.array(labels[:k], dtype=float)

    weights = 1 / np.log2(np.arange(2, len(labels_array) + 2))

    dcg_contribution = labels_array * weights
    dcg = float(np.sum(dcg_contribution))

    ideal_relevant_count = min(int(total_relevant), k)

    ideal_labels = np.array(
        [1] * ideal_relevant_count
        + [0] * (len(labels_array) - ideal_relevant_count),
        dtype=float
    )

    idcg_contribution = ideal_labels * weights
    idcg = float(np.sum(idcg_contribution))

    ndcg = 0.0 if idcg == 0 else dcg / idcg

    detail = pd.DataFrame({
        "rank": range(1, len(labels_array) + 1),
        "manual_relevance": labels_array.astype(int),
        "weight": weights,
        "dcg_contribution": dcg_contribution,
        "ideal_relevance": ideal_labels.astype(int),
        "idcg_contribution": idcg_contribution,
    })

    return dcg, idcg, ndcg, detail


# =========================================================
# RECOMMENDATION RESULT
# =========================================================

def get_hybrid_result(
    query,
    k,
    alpha,
    beta,
    gamma,
    lambda_umkm,
    sold_weight,
    review_weight,
    rating_weight,
    discount_weight,
):
    candidates = bm25_candidates(
        query,
        top_n=TOP_N_CANDIDATES
    )

    if candidates.empty:
        return pd.DataFrame()

    ranked = compute_balanced_hybrid(
        candidates,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        lambda_umkm=lambda_umkm,
        popularity_sold_weight=sold_weight,
        popularity_review_weight=review_weight,
        value_rating_weight=rating_weight,
        value_discount_weight=discount_weight,
    )

    final_result = apply_umkm_priority_constraint(
        ranked,
        top_k=k,
        min_umkm_ratio=MIN_UMKM_RATIO
    )

    return final_result.head(k).reset_index(drop=True)


# =========================================================
# SHOW EVALUATION DETAIL
# =========================================================

def show_evaluation_detail(
    query,
    k,
    alpha,
    beta,
    gamma,
    lambda_umkm,
    sold_weight,
    review_weight,
    rating_weight,
    discount_weight,
    label_dict,
    total_relevant_by_query
):
    print("\n=================================================")
    print("DETAIL EVALUASI HASIL REKOMENDASI")
    print("=================================================")
    print(f"Query           : {query}")
    print(f"K               : {k}")
    print(f"Alpha           : {alpha}")
    print(f"Beta            : {beta}")
    print(f"Gamma           : {gamma}")
    print(f"Lambda UMKM     : {lambda_umkm}")
    print(f"Sold weight     : {sold_weight}")
    print(f"Review weight   : {review_weight}")
    print(f"Rating weight   : {rating_weight}")
    print(f"Discount weight : {discount_weight}")

    final_result = get_hybrid_result(
        query=query,
        k=k,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        lambda_umkm=lambda_umkm,
        sold_weight=sold_weight,
        review_weight=review_weight,
        rating_weight=rating_weight,
        discount_weight=discount_weight,
    )

    if final_result.empty:
        print("Tidak ada kandidat produk.")
        return None

    df_labeled = add_manual_relevance(
        final_result,
        query,
        label_dict
    )

    labels = df_labeled["manual_relevance"].astype(int).tolist()
    total_relevant = int(total_relevant_by_query.get(query, 0))

    relevant_at_k = int(np.sum(labels))
    judged_at_k = int(df_labeled["judged"].sum())
    unjudged_at_k = 1 - (judged_at_k / k)

    precision = precision_at_k(labels, k)
    recall = recall_at_k(labels, total_relevant, k)
    f1 = f1_score(precision, recall)
    ndcg = ndcg_at_k(labels, total_relevant, k)

    dcg, idcg, ndcg_manual, ndcg_detail = calculate_dcg_idcg_detail(
        labels,
        total_relevant,
        k
    )

    df_show = df_labeled.copy()
    df_show.insert(0, "rank", range(1, len(df_show) + 1))

    display_cols = [
        "rank",
        "id",
        "name",
        "category",
        "category_breadcrumb",
        "categoryBreadcrumbs",
        "shop_name",
        "shop_city",
        "umkm_label",
        "manual_relevance",
        "judged",
        "matched_key",
    ]

    display_cols = [
        col for col in display_cols
        if col in df_show.columns
    ]

    print("\n=================================================")
    print(f"DAFTAR PRODUK TOP-{k}")
    print("=================================================")
    print(df_show[display_cols].to_string(index=False))

    print("\n=================================================")
    print("PERHITUNGAN METRIK")
    print("=================================================")

    print(f"Jumlah produk pada Top-{k}         : {k}")
    print(f"Jumlah produk relevan pada Top-{k} : {relevant_at_k}")
    print(f"Jumlah produk judged pada Top-{k}  : {judged_at_k}")
    print(f"Unjudged@{k}                       : {unjudged_at_k:.4f}")
    print(f"Total relevant di judged pool      : {total_relevant}")

    print("\nPrecision@K")
    print(f"Precision@{k} = Relevant@K / K")
    print(f"Precision@{k} = {relevant_at_k} / {k}")
    print(f"Precision@{k} = {precision:.4f}")

    print("\nRecall@K")
    print(f"Recall@{k} = Relevant@K / Total Relevant")
    print(f"Recall@{k} = {relevant_at_k} / {total_relevant}")
    print(f"Recall@{k} = {recall:.4f}")

    print("\nF1-score")
    print("F1 = 2 × Precision × Recall / (Precision + Recall)")
    print(f"F1 = {f1:.4f}")

    print("\nNDCG@K")
    print(f"NDCG@{k} = DCG@{k} / IDCG@{k}")
    print(f"DCG@{k}  = {dcg:.4f}")
    print(f"IDCG@{k} = {idcg:.4f}")
    print(f"NDCG@{k} = {ndcg:.4f}")

    print("\n=================================================")
    print("DETAIL PERHITUNGAN NDCG")
    print("=================================================")
    print(ndcg_detail.to_string(index=False))

    return {
        "top_k_products": df_show,
        "ndcg_detail": ndcg_detail,
        "Precision": precision,
        "Recall": recall,
        "F1_score": f1,
        "NDCG": ndcg,
        "Relevant@K": relevant_at_k,
        "Judged@K": judged_at_k,
        "Unjudged@K": unjudged_at_k,
        "Total_Relevant_Judged_Pool": total_relevant,
    }


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    _, label_dict, total_relevant_by_query = load_manual_labels()

    best_config = get_best_config_for_metric(
        summary_file=SUMMARY_FILE,
        metric="Precision",
        k=20
    )

    detail = show_evaluation_detail(
        query="tas kulit wanita",
        k=20,
        alpha=best_config["alpha"],
        beta=best_config["beta"],
        gamma=best_config["gamma"],
        lambda_umkm=best_config["lambda_umkm"],
        sold_weight=best_config["sold_weight"],
        review_weight=best_config["review_weight"],
        rating_weight=best_config["rating_weight"],
        discount_weight=best_config["discount_weight"],
        label_dict=label_dict,
        total_relevant_by_query=total_relevant_by_query
    )