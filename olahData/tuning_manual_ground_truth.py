# =========================================================
# TUNING PARAMETER USING MANUAL POOLED GROUND TRUTH
# =========================================================

import os
import numpy as np
import pandas as pd
from itertools import product
from retrieval.bm25 import bm25_search, bm25_candidates
from reranking.hybrid_rerank import compute_balanced_hybrid, apply_umkm_priority_constraint
from utils import get_candidate_keys, get_primary_product_key
from metrics import precision_at_k, recall_at_k, f1_score, ndcg_at_k, fairness_at_k, unjudged_at_k


# =========================================================
# FILE INPUT / OUTPUT
# =========================================================

MANUAL_LABEL_FILE = "manual_pool_labeling_labeled.csv"

SUMMARY_OUTPUT = "manual_tuning_summary.csv"
BASELINE_OUTPUT = "manual_baseline_bm25.csv"
DIAGNOSTIC_OUTPUT = "manual_label_diagnostics.csv"


# =========================================================
# QUERY UJI
# =========================================================

TEST_QUERIES = [
    "hiasan rumah",
    "kain batik",
    "keripik singkong",
    "kemeja polos pria",
    "tas kulit wanita",
    "kopi khas daerah",
]


# =========================================================
# PARAMETER TUNING
# Kombinasi ini mengikuti file Anda saat ini.
# =========================================================

TOP_N_CANDIDATES = 2000
MIN_UMKM_RATIO = 0.4

K_VALUES = [10, 20, 30, 40, 50]

WEIGHT_CANDIDATES = [
    ("relevance_dominant", 0.50, 0.250, 0.250),
    ("popularity_dominant", 0.250, 0.50, 0.250),
    ("value_dominant", 0.250, 0.250, 0.50),
    ("balanced", 0.34, 0.33, 0.33),
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
# LOAD MANUAL LABELS
# =========================================================

def load_manual_labels(path=MANUAL_LABEL_FILE):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File {path} tidak ditemukan.\n")

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig", engine="python")
        print("File label berhasil dibaca dengan separator ';' dan encoding utf-8-sig.")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", encoding="cp1252", engine="python")
        print("File label berhasil dibaca dengan separator ';' dan encoding cp1252.")

    df.columns = df.columns.str.strip()

    # Hapus kolom kosong akibat Excel/Text-to-Columns
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", case=False, regex=True)].copy()

    if "query" not in df.columns or "manual_relevance" not in df.columns:
        raise ValueError(
            "File label harus memiliki kolom query dan manual_relevance. "
            f"Kolom yang terbaca: {df.columns.tolist()}"
        )

    df["manual_relevance"] = pd.to_numeric(df["manual_relevance"], errors="coerce")
    df = df[df["manual_relevance"].isin([0, 1])].copy()
    df["manual_relevance"] = df["manual_relevance"].astype(int)

    df["primary_product_key"] = df.apply(get_primary_product_key, axis=1)

    label_dict = {}
    total_relevant_by_query = {}
    diagnostic_rows = []

    for query, group in df.groupby("query"):
        query_label_map = {}

        for _, row in group.iterrows():
            label = int(row["manual_relevance"])

            for key in get_candidate_keys(row):
                if key:
                    query_label_map[key] = label

        label_dict[query] = query_label_map
        total_relevant_by_query[query] = int(group["manual_relevance"].sum())

        diagnostic_rows.append({
            "query": query,
            "num_labeled_items": len(group),
            "num_relevant_items": int(group["manual_relevance"].sum()),
            "num_non_relevant_items": int((group["manual_relevance"] == 0).sum()),
        })

    diagnostics = pd.DataFrame(diagnostic_rows)
    diagnostics.to_csv(DIAGNOSTIC_OUTPUT, index=False, encoding="utf-8-sig", sep=";")

    print("Manual labels loaded.")
    print(f"Jumlah label valid: {len(df)}")

    for q, n_rel in total_relevant_by_query.items():
        print(f"- {q}: {n_rel} produk relevan di judged pool")

    missing_queries = sorted(set(TEST_QUERIES) - set(label_dict.keys()))
    if missing_queries:
        print("Query berikut tidak ditemukan dalam file label manual:")
        for q in missing_queries:
            print(f"- {q}")

    print(f"Diagnostic label disimpan: {DIAGNOSTIC_OUTPUT}")

    return df, label_dict, total_relevant_by_query


# =========================================================
# ADD MANUAL RELEVANCE
# =========================================================

def add_manual_relevance(df_result, query, label_dict):
    df_result = df_result.copy()
    q_labels = label_dict.get(query, {})

    manual_labels = []
    judged_flags = []
    matched_keys = []

    for _, row in df_result.iterrows():
        found_label = None
        found_key = ""

        for key in get_candidate_keys(row):
            if key in q_labels:
                found_label = q_labels[key]
                found_key = key
                break

        if found_label is None:
            manual_labels.append(0)      # konservatif: unjudged dianggap tidak relevan
            judged_flags.append(0)
            matched_keys.append("")
        else:
            manual_labels.append(int(found_label))
            judged_flags.append(1)
            matched_keys.append(found_key)

    df_result["manual_relevance"] = manual_labels
    df_result["judged"] = judged_flags
    df_result["matched_key"] = matched_keys

    return df_result


# =========================================================
# METRICS
# =========================================================

def evaluate_with_manual_labels(df_result, query, k, label_dict, total_relevant_by_query):
    df_labeled = add_manual_relevance(df_result.head(k), query, label_dict)

    labels = df_labeled["manual_relevance"].astype(int).tolist()
    total_relevant = int(total_relevant_by_query.get(query, 0))

    p = precision_at_k(labels, k)
    r = recall_at_k(labels, total_relevant, k)

    return {
        "Precision": p,
        "Recall": r,
        "F1_score": f1_score(p, r),
        "NDCG": ndcg_at_k(labels, total_relevant, k),
        "Fairness": fairness_at_k(df_result, k),
        "Unjudged@K": unjudged_at_k(df_labeled, k),
        "Relevant@K": int(np.sum(labels)),
        "Judged@K": int(df_labeled["judged"].sum()),
        "Total_Relevant_Judged_Pool": total_relevant,
    }


# =========================================================
# CANDIDATE CACHE AND BASELINE
# =========================================================

def build_candidate_cache():
    cache = {}
    for query in TEST_QUERIES:
        print(f"Mengambil candidate BM25 untuk query: {query}")
        cache[query] = bm25_candidates(query, top_n=TOP_N_CANDIDATES)
    return cache


def evaluate_bm25_baseline(label_dict, total_relevant_by_query):
    rows = []

    for query in TEST_QUERIES:
        for k in K_VALUES:
            bm25_result = bm25_search(query, topk=k, use_term_filter=False)

            metrics = evaluate_with_manual_labels(
                bm25_result,
                query,
                k,
                label_dict,
                total_relevant_by_query
            )

            rows.append({
                "query": query,
                "method": "BM25",
                "K": k,
                **metrics
            })

    df_baseline = pd.DataFrame(rows)
    df_baseline.to_csv(BASELINE_OUTPUT, index=False, encoding="utf-8-sig", sep=";")
    print(f"Baseline BM25 disimpan: {BASELINE_OUTPUT}")

    return df_baseline


# =========================================================
# RUN TUNING
# =========================================================

def run_tuning():
    _, label_dict, total_relevant_by_query = load_manual_labels()
    candidate_cache = build_candidate_cache()
    evaluate_bm25_baseline(label_dict, total_relevant_by_query)

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
        VALUE_WEIGHT_CANDIDATES
    )

    for k, weight_candidate, lambda_umkm, pop_weight, value_weight in tuning_combinations:
        scenario_group, alpha, beta, gamma = weight_candidate
        sold_weight, review_weight = pop_weight
        rating_weight, discount_weight = value_weight

        for query in TEST_QUERIES:
            counter += 1
            print(f"[{counter}/{total}] query={query} | K={k} | {scenario_group} | lambda={lambda_umkm}")

            candidates = candidate_cache[query]

            if candidates.empty:
                rows.append({
                    "query": query,
                    "scenario_group": scenario_group,
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
                "scenario_group": scenario_group,
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

    summary.to_csv(SUMMARY_OUTPUT, index=False, encoding="utf-8-sig", sep=";")
    print(f"Summary disimpan: {SUMMARY_OUTPUT}")

    return summary


# =========================================================
# MAIN EXECUTION
# =========================================================

if __name__ == "__main__":
    df_raw = run_tuning()
    summary = build_summary(df_raw)

    print("\n===== RINGKASAN HASIL TUNING =====")
    print(summary.head(20).to_string(index=False))

    print("\nFile hasil tuning tersimpan:")
    print(f"- {SUMMARY_OUTPUT}")
    print(f"- {BASELINE_OUTPUT}")
    print(f"- {DIAGNOSTIC_OUTPUT}")
