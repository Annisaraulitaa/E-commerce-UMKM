# =========================================================
# IMPORT LIBRARY
# =========================================================
import os
import re
import numpy as np
import pandas as pd
from itertools import product

from retrieval.bm25 import bm25_candidates
from reranking.hybrid_rerank import (
    compute_balanced_hybrid,
    apply_umkm_priority_constraint
)


# =========================================================
# FILE OUTPUT
# =========================================================
CHECKPOINT_FILE = "joint_tuning_checkpoint_scenario_raw_v2.csv"
SUMMARY_FILE = "joint_tuning_summary_scenario_5metrics_v2.csv"
SCENARIO_GROUP_SUMMARY_FILE = "joint_tuning_scenario_group_summary_5metrics_v2.csv"
SCENARIO_NAME_SUMMARY_FILE = "joint_tuning_scenario_name_summary_5metrics_v2.csv"
SCENARIO_SPLIT_DIR = "scenario_summaries"


# =========================================================
# KONFIGURASI EKSPERIMEN
# =========================================================
TEST_QUERIES = [
    "kopi khas daerah",
    "kopi instan sachet",
    "baju batik pria",
    "tas wanita kulit",
    "hiasan rumah handmade",
    "oleh-oleh khas daerah",
]

TOP_N_CANDIDATES = 2000
MIN_UMKM_RATIO = 0.4

# dalam sistem rekomendasi nyata, pengguna tidak akan melihat ribuan produk. 
# Mereka hanya melihat 5, 10, atau 20 produk pertama.
K_VALUES = [5, 10, 15, 20, 25] 

# =========================================================
# KOMBINASI ALPHA, BETA, GAMMA BERDASARKAN SKENARIO
# =========================================================
# Format:
# (scenario_group, scenario_name, alpha, beta, gamma)
#
# alpha = relevance_score / BM25
# beta  = popularity_score
# gamma = value_score
#
# alpha + beta + gamma = 1
# =========================================================
WEIGHT_CANDIDATES = [
    # Relevance dominant (alpha lebih besar)
    ("relevance_dominant", "rel_45", 0.45, 0.275, 0.275),
    ("relevance_dominant", "rel_50", 0.50, 0.250, 0.250),
    ("relevance_dominant", "rel_55", 0.55, 0.225, 0.225),
    ("relevance_dominant", "rel_60", 0.60, 0.200, 0.200),
    ("relevance_dominant", "rel_65", 0.65, 0.175, 0.175),
    ("relevance_dominant", "rel_70", 0.70, 0.150, 0.150),

    # Popularity dominant (beta lebih besar)
    ("popularity_dominant", "pop_45", 0.275, 0.45, 0.275),
    ("popularity_dominant", "pop_50", 0.250, 0.50, 0.250),
    ("popularity_dominant", "pop_55", 0.225, 0.55, 0.225),
    ("popularity_dominant", "pop_60", 0.200, 0.60, 0.200),
    ("popularity_dominant", "pop_65", 0.175, 0.65, 0.175),
    ("popularity_dominant", "pop_70", 0.150, 0.70, 0.150),

    # Value dominant (gamma lebih besar)
    ("value_dominant", "val_45", 0.275, 0.275, 0.45),
    ("value_dominant", "val_50", 0.250, 0.250, 0.50),
    ("value_dominant", "val_55", 0.225, 0.225, 0.55),
    ("value_dominant", "val_60", 0.200, 0.200, 0.60),
    ("value_dominant", "val_65", 0.175, 0.175, 0.65),
    ("value_dominant", "val_70", 0.150, 0.150, 0.70),

    # Balanced
    ("balanced", "bal_equal", 0.34, 0.33, 0.33),
    ("balanced", "bal_alpha", 0.36, 0.32, 0.32),
    ("balanced", "bal_beta", 0.32, 0.36, 0.32),
    ("balanced", "bal_gamma", 0.32, 0.32, 0.36),
]


# =========================================================
# LAMBDA UMKM
# =========================================================
# Lambda tidak dimulai dari 0 karena lambda_umkm merupakan
# bagian dari mekanisme boost UMKM pada sistem hybrid.
# Nilainya dibuat kecil-sedang karena sistem juga memakai UMKM-first.
# =========================================================
LAMBDA_VALUES = [0.03, 0.05, 0.07, 0.1, 0.15]


# =========================================================
# BOBOT INTERNAL POPULARITY DAN VALUE
# =========================================================
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
# METRIC FUNCTIONS
# =========================================================
def basic_tokens(text):
    """
    Tokenisasi sederhana agar konsisten dengan BM25:
    lowercase, hapus simbol, rapikan spasi, lalu split.
    """
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.split() if text else []


def get_query_tokens(query):
    """
    Mengubah query menjadi token unigram.
    """
    return basic_tokens(query)


def build_eval_text(df):
    """
    Membentuk teks evaluasi agar konsisten dengan dokumen BM25.

    Jika kolom 'doc' sudah ada dari bm25_candidates(), gunakan 'doc'.
    Jika tidak ada, bentuk ulang dari:
    name_clean + name_clean + category_clean + city_clean

    Struktur ini mengikuti dokumen BM25 pada baseline.
    """
    df = df.copy()

    if "doc" in df.columns:
        return df["doc"].fillna("").astype(str)

    for col in ["name_clean", "category_clean", "city_clean"]:
        if col not in df.columns:
            df[col] = ""
    
    eval_text = (
        df["name_clean"].fillna("").astype(str) + " " +
        df["name_clean"].fillna("").astype(str) + " " +
        df["category_clean"].fillna("").astype(str) + " " +
        df["city_clean"].fillna("").astype(str)
    )

    return eval_text


def relevant_mask_df(df, query):
    """
    Menentukan apakah produk relevan terhadap query untuk evaluasi metrik.

    Aturan relevansi:
    - Query 1-2 token:
    semua token harus muncul.

    - Query 3 token:
    minimal 2 dari 3 token harus muncul.

    - Query > 3 token:
    minimal n-1 token harus muncul, dengan batas minimum 3 token.
    """
    q_tokens = get_query_tokens(query)
    n_tokens = len(q_tokens)

    if n_tokens == 0:
        return pd.Series([False] * len(df), index=df.index)

    if n_tokens <= 2:
        min_match = n_tokens
    elif n_tokens == 3:
        min_match = 2
    else:
        min_match = max(3, n_tokens - 1)

    eval_text = build_eval_text(df)

    def is_relevant(text):
        text_tokens = set(basic_tokens(text))
        match_count = sum(token in text_tokens for token in q_tokens)
        return match_count >= min_match

    return eval_text.apply(is_relevant)


def precision_at_k(df, query, k):
    """
    Precision@K:
    jumlah produk relevan pada top-K / jumlah produk top-K.
    """
    df_k = df.head(k).copy()

    if len(df_k) == 0:
        return 0.0

    rel_mask = relevant_mask_df(df_k, query)
    return float(rel_mask.sum() / len(df_k))


def recall_at_k(df_topk, df_all, query, k):
    """
    Recall@K:
    jumlah produk relevan pada top-K /
    jumlah seluruh produk relevan dalam candidate pool BM25.
    """
    if len(df_all) == 0:
        return 0.0

    all_rel = relevant_mask_df(df_all, query)
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask_df(df_topk.head(k), query)
    return float(topk_rel.sum() / total_relevant)


def f1_at_k(precision, recall):
    """
    F1-score:
    harmonic mean dari Precision dan Recall.
    """
    if precision + recall == 0:
        return 0.0

    return float(2 * precision * recall / (precision + recall))


def ndcg_at_k(df, query, k):
    """
    NDCG@K:
    mengukur kualitas urutan ranking berdasarkan relevansi biner.

    Relevance:
    1 = relevan
    0 = tidak relevan
    """
    df_k = df.head(k).copy()

    if len(df_k) == 0:
        return 0.0

    rel = relevant_mask_df(df_k, query).astype(int).to_numpy()

    discounts = 1 / np.log2(np.arange(2, len(rel) + 2))
    dcg = np.sum(rel * discounts)

    ideal_rel = np.sort(rel)[::-1]
    idcg = np.sum(ideal_rel * discounts)

    if idcg == 0:
        return 0.0

    return float(dcg / idcg)


def prepare_label(df):
    df = df.copy()

    if "umkm_label" not in df.columns:
        df["umkm_label"] = 0

    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0
    })

    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)

    return df


def fairness_at_k(df, k):
    """
    Fairness@K:
    proporsi produk UMKM pada top-K.
    """
    df = prepare_label(df)
    df_k = df.head(k).copy()

    if len(df_k) == 0:
        return 0.0

    return float(df_k["umkm_label"].mean())


def evaluate_one_query(df_result, df_candidates, query, k):
    """
    Menghitung 5 metrik evaluasi untuk satu query.
    """
    precision = precision_at_k(df_result, query, k)
    recall = recall_at_k(df_result, df_candidates, query, k)
    f1 = f1_at_k(precision, recall)
    ndcg = ndcg_at_k(df_result, query, k)
    fairness = fairness_at_k(df_result, k)

    return {
        "Precision": precision,
        "Recall": recall,
        "F1_score": f1,
        "NDCG": ndcg,
        "Fairness": fairness
    }


# =========================================================
# CHECKPOINT FUNCTIONS
# =========================================================
def make_experiment_id(
    query,
    k,
    scenario_group,
    scenario_name,
    alpha,
    beta,
    gamma,
    lambda_umkm,
    sold_weight,
    review_weight,
    rating_weight,
    discount_weight
):
    """
    Membuat ID unik untuk setiap eksperimen.
    ID ini dipakai agar eksperimen yang sudah selesai
    tidak dijalankan ulang.
    """
    return (
        f"{query}|"
        f"K={k}|"
        f"scenario_group={scenario_group}|scenario_name={scenario_name}|"
        f"a={alpha}|b={beta}|g={gamma}|l={lambda_umkm}|"
        f"sold={sold_weight}|review={review_weight}|"
        f"rating={rating_weight}|discount={discount_weight}"
    )


def load_completed_experiments():
    """
    Membaca checkpoint yang sudah ada.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return pd.DataFrame(), set()

    df_checkpoint = pd.read_csv(CHECKPOINT_FILE)

    if "experiment_id" not in df_checkpoint.columns:
        return df_checkpoint, set()

    completed_ids = set(df_checkpoint["experiment_id"].astype(str).tolist())

    print(f"Checkpoint ditemukan: {len(completed_ids)} eksperimen sudah selesai.")

    return df_checkpoint, completed_ids


def append_checkpoint(row):
    """
    Menyimpan satu hasil eksperimen ke file checkpoint.
    """
    df_row = pd.DataFrame([row])

    file_exists = os.path.exists(CHECKPOINT_FILE)

    df_row.to_csv(
        CHECKPOINT_FILE,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig"
    )


# =========================================================
# PRECOMPUTE CANDIDATES
# =========================================================
def build_candidate_cache():
    """
    Candidate pool BM25 diambil satu kali per query
    agar eksperimen lebih efisien.
    """
    candidate_cache = {}

    for query in TEST_QUERIES:
        print(f"Mengambil candidate BM25 untuk query: {query}")

        candidates = bm25_candidates(
            query,
            top_n=TOP_N_CANDIDATES
        )

        candidate_cache[query] = prepare_label(candidates)

    return candidate_cache


# =========================================================
# JOINT TUNING WITH CHECKPOINT
# =========================================================
def run_joint_tuning_with_checkpoint():
    _, completed_ids = load_completed_experiments()

    candidate_cache = build_candidate_cache()

    total_evaluations = (
        len(K_VALUES)
        * len(WEIGHT_CANDIDATES)
        * len(LAMBDA_VALUES)
        * len(POPULARITY_WEIGHT_CANDIDATES)
        * len(VALUE_WEIGHT_CANDIDATES)
        * len(TEST_QUERIES)
    )

    print(f"\nTotal evaluasi query: {total_evaluations}")
    print("Tuning dimulai...\n")

    processed_counter = 0
    skipped_counter = 0
    new_counter = 0

    for (
        k,
        weight_main,
        lambda_umkm,
        pop_weights,
        value_weights
    ) in product(
        K_VALUES,
        WEIGHT_CANDIDATES,
        LAMBDA_VALUES,
        POPULARITY_WEIGHT_CANDIDATES,
        VALUE_WEIGHT_CANDIDATES
    ):

        scenario_group, scenario_name, alpha, beta, gamma = weight_main
        sold_weight, review_weight = pop_weights
        rating_weight, discount_weight = value_weights

        for query in TEST_QUERIES:
            processed_counter += 1

            experiment_id = make_experiment_id(
                query=query,
                k=k,
                scenario_group=scenario_group,
                scenario_name=scenario_name,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
                lambda_umkm=lambda_umkm,
                sold_weight=sold_weight,
                review_weight=review_weight,
                rating_weight=rating_weight,
                discount_weight=discount_weight
            )

            # Jika sudah ada di checkpoint, eksperimen dilewati.
            if experiment_id in completed_ids:
                skipped_counter += 1
                continue

            candidates = candidate_cache[query]

            if len(candidates) == 0:
                row = {
                    "experiment_id": experiment_id,
                    "query": query,
                    "scenario_group": scenario_group,
                    "scenario_name": scenario_name,
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
                    "status": "empty_candidates"
                }

                append_checkpoint(row)
                completed_ids.add(experiment_id)
                new_counter += 1
                continue

            # 1. Hitung skor hybrid
            hybrid_ranked = compute_balanced_hybrid(
                candidates,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
                lambda_umkm=lambda_umkm,
                popularity_sold_weight=sold_weight,
                popularity_review_weight=review_weight,
                value_rating_weight=rating_weight,
                value_discount_weight=discount_weight
            )

            # 2. Terapkan UMKM-first / UMKM priority constraint
            final_result = apply_umkm_priority_constraint(
                hybrid_ranked,
                top_k=k,
                min_umkm_ratio=MIN_UMKM_RATIO
            )

            final_result = prepare_label(final_result)

            # 3. Evaluasi hasil top-K
            metrics = evaluate_one_query(
                df_result=final_result,
                df_candidates=candidates,
                query=query,
                k=k
            )

            row = {
                "experiment_id": experiment_id,
                "query": query,
                "scenario_group": scenario_group,
                "scenario_name": scenario_name,
                "K": k,
                "alpha": alpha,
                "beta": beta,
                "gamma": gamma,
                "lambda_umkm": lambda_umkm,
                "sold_weight": sold_weight,
                "review_weight": review_weight,
                "rating_weight": rating_weight,
                "discount_weight": discount_weight,
                "Precision": metrics["Precision"],
                "Recall": metrics["Recall"],
                "F1_score": metrics["F1_score"],
                "NDCG": metrics["NDCG"],
                "Fairness": metrics["Fairness"],
                "status": "done"
            }

            append_checkpoint(row)
            completed_ids.add(experiment_id)
            new_counter += 1

            if new_counter % 100 == 0:
                print(
                    f"Progress baru: {new_counter} | "
                    f"Dilewati dari checkpoint: {skipped_counter} | "
                    f"Total dicek: {processed_counter}/{total_evaluations}"
                )

    print("\nTuning selesai.")
    print(f"Data baru disimpan   : {new_counter}")
    print(f"Data dilewati/resume : {skipped_counter}")

    return pd.read_csv(CHECKPOINT_FILE)


# =========================================================
# BUILD SUMMARY OUTPUTS
# =========================================================
def build_summary(df_raw):
    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness"
    ]

    group_cols = [
        "scenario_group",
        "scenario_name",
        "K",
        "alpha",
        "beta",
        "gamma",
        "lambda_umkm",
        "sold_weight",
        "review_weight",
        "rating_weight",
        "discount_weight"
    ]

    df_raw = df_raw[df_raw["status"] == "done"].copy()

    summary = (
        df_raw
        .groupby(group_cols)[metric_cols]
        .mean()
        .reset_index()
    )

    summary["overall_score"] = summary[metric_cols].mean(axis=1)

    return summary


def build_scenario_group_summary(summary):
    """
    Membuat rata-rata metrik untuk tiap scenario_group:
    relevance_dominant, popularity_dominant, value_dominant, balanced.
    """
    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "overall_score"
    ]

    scenario_group_summary = (
        summary
        .groupby("scenario_group")[metric_cols]
        .mean()
        .reset_index()
    )

    return scenario_group_summary


def build_scenario_name_summary(summary):
    """
    Membuat rata-rata metrik untuk tiap scenario_name:
    rel_45, rel_50, pop_45, val_45, bal_equal, dst.

    Output ini cocok dipakai oleh code plot terpisah.
    """
    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "overall_score"
    ]

    scenario_name_summary = (
        summary
        .groupby(["scenario_group", "scenario_name"])[metric_cols]
        .mean()
        .reset_index()
    )

    return scenario_name_summary


def save_summary_per_scenario(summary):
    """
    Menyimpan CSV terpisah untuk masing-masing scenario_group.

    Output:
    - scenario_summaries/summary_relevance_dominant.csv
    - scenario_summaries/summary_popularity_dominant.csv
    - scenario_summaries/summary_value_dominant.csv
    - scenario_summaries/summary_balanced.csv
    """
    os.makedirs(SCENARIO_SPLIT_DIR, exist_ok=True)

    scenario_groups = summary["scenario_group"].dropna().unique()

    saved_files = []

    for scenario in scenario_groups:
        df_scenario = summary[
            summary["scenario_group"] == scenario
        ].copy()

        output_file = os.path.join(
            SCENARIO_SPLIT_DIR,
            f"summary_{scenario}.csv"
        )

        df_scenario.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )

        saved_files.append(output_file)

    return saved_files


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    df_raw = run_joint_tuning_with_checkpoint()

    summary = build_summary(df_raw)
    scenario_group_summary = build_scenario_group_summary(summary)
    scenario_name_summary = build_scenario_name_summary(summary)
    scenario_split_files = save_summary_per_scenario(summary)

    summary.to_csv(
        SUMMARY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    scenario_group_summary.to_csv(
        SCENARIO_GROUP_SUMMARY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    scenario_name_summary.to_csv(
        SCENARIO_NAME_SUMMARY_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print("\n===== SUMMARY PER KOMBINASI PARAMETER =====")
    print(summary.head(20).to_string(index=False))

    print("\n===== SUMMARY PER SCENARIO GROUP =====")
    print(scenario_group_summary.to_string(index=False))

    print("\n===== SUMMARY PER SCENARIO NAME =====")
    print(scenario_name_summary.to_string(index=False))

    print("\nFile tersimpan:")
    print(f"- {CHECKPOINT_FILE}")
    print(f"- {SUMMARY_FILE}")
    print(f"- {SCENARIO_GROUP_SUMMARY_FILE}")
    print(f"- {SCENARIO_NAME_SUMMARY_FILE}")

    print("\nFile summary per skenario:")
    for file_path in scenario_split_files:
        print(f"- {file_path}")