# =========================================================
# TUNING PARAMETER RELEVANCE DOMINANT ONLY 
# USING MANUAL POOLED GROUND TRUTH
# =========================================================
# Input: manual_pool_labeling_labeled.csv
# Output: manual_tuning_summary_relevance_only.csv
# =========================================================

import os
import re
import numpy as np
import pandas as pd
from itertools import product
from retrieval.bm25 import bm25_candidates
from reranking.hybrid_rerank import compute_balanced_hybrid, apply_umkm_priority_constraint
from tuning_manual_ground_truth import (
    TEST_QUERIES,
    TOP_N_CANDIDATES,
    MIN_UMKM_RATIO,
    K_VALUES,
    load_manual_labels,
    evaluate_with_manual_labels,
)

# File input
MANUAL_LABEL_FILE = "manual_pool_labeling_labeled.csv"
# File output
SUMMARY_OUTPUT = "manual_tuning_summary_relevance_only.csv"


# =========================================================
# PARAMETER TUNING RELEVANCE DOMINANT
# =========================================================
WEIGHT_CANDIDATES = [
    (0.50, 0.25, 0.25),
    (0.55, 0.225, 0.225),
    (0.60, 0.20, 0.20),
    (0.65, 0.175, 0.175),
    (0.70, 0.15, 0.15),
]

LAMBDA_VALUES = [0.03, 0.05, 0.07, 0.10, 0.15]

POPULARITY_WEIGHT_CANDIDATES = [
    (0.4, 0.6),
    (0.5, 0.5),
    (0.6, 0.4),
    (0.7, 0.3),
]

VALUE_WEIGHT_CANDIDATES = [
    (0.4, 0.6),
    (0.5, 0.5),
    (0.6, 0.4),
    (0.7, 0.3),
]


# =========================================================
# RETRIEVAL CACHE AND BASELINE
# =========================================================

def build_candidate_cache():
    cache = {}
    for query in TEST_QUERIES:
        print(f"Mengambil candidate BM25 untuk query: {query}")
        cache[query] = bm25_candidates(query, top_n=TOP_N_CANDIDATES)
    return cache


# =========================================================
# RUN TUNING
# =========================================================

def run_tuning():
    _, label_dict, total_relevant_by_query = load_manual_labels()
    candidate_cache = build_candidate_cache()

    rows = []

    total = (
        len(K_VALUES)
        * len(WEIGHT_CANDIDATES)
        * len(LAMBDA_VALUES)
        * len(POPULARITY_WEIGHT_CANDIDATES)
        * len(VALUE_WEIGHT_CANDIDATES)
        * len(TEST_QUERIES)
    )

    counter = 0

    tuning_combinations = product(
        K_VALUES,
        WEIGHT_CANDIDATES,
        LAMBDA_VALUES,
        POPULARITY_WEIGHT_CANDIDATES,
        VALUE_WEIGHT_CANDIDATES,
    )

    for k, weight_candidate, lambda_umkm, pop_weight, value_weight in tuning_combinations:
        alpha, beta, gamma = weight_candidate
        sold_weight, review_weight = pop_weight
        rating_weight, discount_weight = value_weight

        for query in TEST_QUERIES:
            counter += 1

            print(
                f"[{counter}/{total}] "
                f"query={query} | "
                f"K={k} | "
                f"alpha={alpha} | "
                f"beta={beta} | "
                f"gamma={gamma} | "
                f"lambda={lambda_umkm}"
            )

            candidates = candidate_cache[query]

            if candidates.empty:
                rows.append({
                    "query": query,
                    "scenario_group": "relevance_dominant",
                    "K": k,
                    "alpha": alpha,
                    "beta": beta,
                    "gamma": gamma,
                    "lambda_umkm": lambda_umkm,
                    "sold_weight": sold_weight,
                    "review_weight": review_weight,
                    "rating_weight": rating_weight,
                    "discount_weight": discount_weight,
                    "Precision": 0.0,
                    "Recall": 0.0,
                    "F1_score": 0.0,
                    "NDCG": 0.0,
                    "Fairness": 0.0,
                    "Unjudged@K": 1.0,
                    "Relevant@K": 0,
                    "Judged@K": 0,
                    "status": "empty_candidates",
                })
                continue

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

            metrics = evaluate_with_manual_labels(
                final_result,
                query,
                k,
                label_dict,
                total_relevant_by_query
            )

            rows.append({
                "query": query,
                "scenario_group": "relevance_dominant",
                "K": k,
                "alpha": alpha,
                "beta": beta,
                "gamma": gamma,
                "lambda_umkm": lambda_umkm,
                "sold_weight": sold_weight,
                "review_weight": review_weight,
                "rating_weight": rating_weight,
                "discount_weight": discount_weight,
                **metrics,
                "status": "done",
            })

    return pd.DataFrame(rows)


# =========================================================
# BUILD SUMMARY
# =========================================================

def build_summary(df_raw):
    df_done = df_raw[df_raw["status"] == "done"].copy()

    group_cols = [
        "scenario_group",
        "K",
        "alpha",
        "beta",
        "gamma",
        "lambda_umkm",
        "sold_weight",
        "review_weight",
        "rating_weight",
        "discount_weight",
    ]

    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "Unjudged@K",
        "Relevant@K",
        "Judged@K",
    ]

    summary = (
        df_done
        .groupby(group_cols)[metric_cols]
        .mean()
        .reset_index()
    )

    summary.to_csv(
        SUMMARY_OUTPUT,
        index=False,
        encoding="utf-8-sig",
        sep=";"
    )

    print(f"Summary relevance dominant disimpan: {SUMMARY_OUTPUT}")

    return summary


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    df_raw = run_tuning()
    summary = build_summary(df_raw)

    print("\n===== RINGKASAN TUNING RELEVANCE DOMINANT =====")
    print(summary.head(20).to_string(index=False))

    print("\nFile hasil tuning tersimpan:")
    print(f"- {SUMMARY_OUTPUT}")
