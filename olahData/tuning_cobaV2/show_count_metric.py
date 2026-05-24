# =========================================================
# SHOW DETAIL METRIC CALCULATION FOR ALL QUERIES
# =========================================================
# Output:
# 1. "detail_all_queries_top{K_DETAIL}.csv"
#    Kolom: query, name, category_breadcumb, umkm_label, manual_relevance
#
# 2. "summary_metric_all_queries_top{K_DETAIL}.csv"
#    Kolom: query, Relevant@K, Total_Relevant_Judged_Pool,
#           Precision, Recall, F1_score, NDCG
# =========================================================

import numpy as np
import pandas as pd

from retrieval.bm25 import bm25_candidates
from reranking.hybrid_rerank import (
    compute_balanced_hybrid,
    apply_umkm_priority_constraint
)

from tuning_manual_ground_truth import (
    TEST_QUERIES,
    TOP_N_CANDIDATES,
    MIN_UMKM_RATIO,
    load_manual_labels,
    add_manual_relevance,
    evaluate_with_manual_labels,
)

from utils import get_col


# =========================================================
# CONFIG
# =========================================================

K_DETAIL = 20

# Konfigurasi parameter yang ingin ditampilkan
ALPHA = 0.60
BETA = 0.20
GAMMA = 0.20
LAMBDA_UMKM = 0.07

SOLD_WEIGHT = 0.4
REVIEW_WEIGHT = 0.6

RATING_WEIGHT = 0.6
DISCOUNT_WEIGHT = 0.4

DETAIL_OUTPUT_FILE = f"detail_all_queries_top{K_DETAIL}.csv"
SUMMARY_OUTPUT_FILE = f"summary_metric_all_queries_top{K_DETAIL}.csv"


# =========================================================
# RECOMMENDATION RESULT
# =========================================================

def get_hybrid_result(query: str, k: int) -> pd.DataFrame:
    candidates = bm25_candidates(
        query,
        top_n=TOP_N_CANDIDATES
    )

    if candidates.empty:
        return pd.DataFrame()

    ranked = compute_balanced_hybrid(
        candidates,
        alpha=ALPHA,
        beta=BETA,
        gamma=GAMMA,
        lambda_umkm=LAMBDA_UMKM,
        popularity_sold_weight=SOLD_WEIGHT,
        popularity_review_weight=REVIEW_WEIGHT,
        value_rating_weight=RATING_WEIGHT,
        value_discount_weight=DISCOUNT_WEIGHT,
    )

    final_result = apply_umkm_priority_constraint(
        ranked,
        top_k=k,
        min_umkm_ratio=MIN_UMKM_RATIO
    )

    return final_result.head(k).reset_index(drop=True)


# =========================================================
# DETAIL ROWS
# =========================================================

def build_detail_rows(df_labeled: pd.DataFrame, query: str) -> pd.DataFrame:
    rows = []

    for rank, (_, row) in enumerate(df_labeled.iterrows(), start=1):
        rows.append({
            "query": query,
            "rank": rank,
            "name": get_col(row, ["name"], ""),
            "category_breadcrumb": get_col(
                row,
                ["category_breadcrumb", "category", "categoryBreadcrumbs"],
                ""
            ),
            "shop_name": get_col(row, ["shop_name"], ""),
            "shop_city": get_col(row, ["shop_city"], ""),
            "umkm_label": get_col(row, ["umkm_label"], 0),
            "manual_relevance": int(row["manual_relevance"]),
        })

    return pd.DataFrame(rows)


# =========================================================
# EVALUATE ONE QUERY
# =========================================================

def evaluate_query(query, k, label_dict, total_relevant_by_query):
    final_result = get_hybrid_result(query, k)

    if final_result.empty:
        summary_row = {
            "query": query,
            "Relevant@K": 0,
            "Judged@K": 0,
            "Precision": 0.0,
            "Recall": 0.0,
            "F1_score": 0.0,
            "NDCG": 0.0,
            "Fairness": 0.0,
            "Unjudged@K": 1.0,
        }
        return pd.DataFrame(), summary_row

    df_labeled = add_manual_relevance(
        final_result,
        query,
        label_dict
    )

    metrics = evaluate_with_manual_labels(
        final_result,
        query,
        k,
        label_dict,
        total_relevant_by_query
    )

    summary_row = {
        "query": query,
        "Relevant@K": metrics["Relevant@K"],
        "Judged@K": metrics["Judged@K"],
        "Precision": metrics["Precision"],
        "Recall": metrics["Recall"],
        "F1_score": metrics["F1_score"],
        "NDCG": metrics["NDCG"],
        "Fairness": metrics["Fairness"],
        "Unjudged@K": metrics["Unjudged@K"],
    }

    df_detail = build_detail_rows(df_labeled, query)

    return df_detail, summary_row


# =========================================================
# RUN DETAIL EVALUATION
# =========================================================

def run_detail_evaluation():
    _, label_dict, total_relevant_by_query = load_manual_labels()

    all_detail_frames = []
    summary_rows = []

    for query in TEST_QUERIES:
        print(f"\nMemproses query: {query}")

        df_detail, summary_row = evaluate_query(
            query=query,
            k=K_DETAIL,
            label_dict=label_dict,
            total_relevant_by_query=total_relevant_by_query
        )

        if not df_detail.empty:
            all_detail_frames.append(df_detail)

        summary_rows.append(summary_row)

    df_all_detail = pd.concat(
        all_detail_frames,
        ignore_index=True
    ) if all_detail_frames else pd.DataFrame()

    df_summary = pd.DataFrame(summary_rows)

    average_row = {
        "query": "AVERAGE",
        "Relevant@K": df_summary["Relevant@K"].mean(),
        "Judged@K": df_summary["Judged@K"].mean(),
        "Precision": df_summary["Precision"].mean(),
        "Recall": df_summary["Recall"].mean(),
        "F1_score": df_summary["F1_score"].mean(),
        "NDCG": df_summary["NDCG"].mean(),
        "Fairness": df_summary["Fairness"].mean(),
        "Unjudged@K": df_summary["Unjudged@K"].mean(),
    }

    df_summary = pd.concat(
        [df_summary, pd.DataFrame([average_row])],
        ignore_index=True
    )

    df_all_detail.to_csv(
        DETAIL_OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig",
        sep=";"
    )

    df_summary.to_csv(
        SUMMARY_OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig",
        sep=";"
    )

    print("\n=================================================")
    print("DETAIL PRODUK TOP-K")
    print("=================================================")
    print(df_all_detail.to_string(index=False))

    print("\n=================================================")
    print("SUMMARY METRIK PER QUERY")
    print("=================================================")
    print(df_summary.to_string(index=False))

    print("\nFile berhasil disimpan:")
    print(f"- {DETAIL_OUTPUT_FILE}")
    print(f"- {SUMMARY_OUTPUT_FILE}")

    return df_all_detail, df_summary


if __name__ == "__main__":
    run_detail_evaluation()