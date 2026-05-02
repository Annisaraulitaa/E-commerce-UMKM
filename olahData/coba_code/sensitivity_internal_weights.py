import numpy as np
import pandas as pd
import re
from retrieval.bm25 import bm25_candidates
from reranking.hybrid_rerank import balanced_hybrid_search


# =========================================================
# METRIC FUNCTIONS
# =========================================================
def relevant_mask(series, query, threshold=0.5):
    q_tokens = re.findall(r"\w+", query.lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k=20, threshold=0.5):
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return float(mask.mean())


def recall_at_k(df_topk, df_all, query, k=20, threshold=0.5):
    all_rel = relevant_mask(df_all["name_clean"], query, threshold)
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return float(topk_rel.sum() / total_relevant)


def fairness_at_k(df, k=20):
    return float(df.head(k)["umkm_label"].mean())


def ndcg_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return float(dcg / idcg) if idcg > 0 else 0.0


def exposure_disparity_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rank"] = np.arange(1, len(df_k) + 1)
    df_k["exposure"] = 1 / np.log2(df_k["rank"] + 1)

    umkm_exp = df_k[df_k["umkm_label"] == 1]["exposure"].mean()
    non_umkm_exp = df_k[df_k["umkm_label"] == 0]["exposure"].mean()

    if np.isnan(umkm_exp):
        umkm_exp = 0.0
    if np.isnan(non_umkm_exp):
        non_umkm_exp = 0.0

    return float(abs(umkm_exp - non_umkm_exp))


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
# CONFIG FINAL PARAMETER
# =========================================================
TEST_QUERIES = [
    "kopi khas daerah",
    "kopi instan sachet",
    "baju batik pria",
    "tas wanita kulit",
    "keripik singkong",
    "hiasan rumah handmade",
]

TOP_N_CANDIDATES = 2000
TOP_K = 40
MIN_UMKM_RATIO = 0.4

# Parameter final tetap
ALPHA = 0.6
BETA = 0.15
GAMMA = 0.25
LAMBDA_UMKM = 0.1

# Variasi bobot popularity
POPULARITY_WEIGHT_CANDIDATES = [
    (0.4, 0.6),
    (0.5, 0.5),
    (0.6, 0.4),
    (0.7, 0.3),
]

# Variasi bobot value_score
VALUE_WEIGHT_CANDIDATES = [
    (0.4, 0.6),
    (0.5, 0.5),
    (0.6, 0.4),
    (0.7, 0.3),
]


# =========================================================
# EKSPERIMEN A: SENSITIVITY POPULARITY
# Value tetap = 0.7 / 0.3
# =========================================================
def run_popularity_sensitivity():
    rows = []

    fixed_value_rating_w = 0.7
    fixed_value_discount_w = 0.3

    for sold_w, review_w in POPULARITY_WEIGHT_CANDIDATES:
        print(f"\n[Popularity Test] sold={sold_w}, review={review_w}")

        for q in TEST_QUERIES:
            candidate_pool = prepare_label(
                bm25_candidates(q, top_n=TOP_N_CANDIDATES)
            )

            # Catatan:
            # Bagian ini mengasumsikan balanced_hybrid_search / hybrid_rerank.py
            # sudah mendukung parameter tambahan:
            # popularity_sold_weight
            # popularity_review_weight
            # value_rating_weight
            # value_discount_weight
            hybrid_res = prepare_label(
                balanced_hybrid_search(
                    query=q,
                    top_n_candidates=TOP_N_CANDIDATES,
                    top_k_results=TOP_K,
                    min_umkm_ratio=MIN_UMKM_RATIO,
                    lambda_umkm=LAMBDA_UMKM,
                    alpha=ALPHA,
                    beta=BETA,
                    gamma=GAMMA,
                    popularity_sold_weight=sold_w,
                    popularity_review_weight=review_w,
                    value_rating_weight=fixed_value_rating_w,
                    value_discount_weight=fixed_value_discount_w,
                )
            )

            rows.append({
                "query": q,
                "experiment_type": "popularity",
                "sold_weight": sold_w,
                "review_weight": review_w,
                "rating_weight": fixed_value_rating_w,
                "discount_weight": fixed_value_discount_w,
                "Precision@40": precision_at_k(hybrid_res, q, TOP_K),
                "Recall@40": recall_at_k(hybrid_res, candidate_pool, q, TOP_K),
                "NDCG@40": ndcg_at_k(hybrid_res, TOP_K),
                "Fairness@40": fairness_at_k(hybrid_res, TOP_K),
                "ExposureDisparity@40": exposure_disparity_at_k(hybrid_res, TOP_K),
            })

    return pd.DataFrame(rows)


# =========================================================
# EKSPERIMEN B: SENSITIVITY VALUE_SCORE
# Popularity tetap = 0.6 / 0.4
# =========================================================
def run_value_sensitivity():
    rows = []

    fixed_pop_sold_w = 0.6
    fixed_pop_review_w = 0.4

    for rating_w, discount_w in VALUE_WEIGHT_CANDIDATES:
        print(f"\n[Value Test] rating={rating_w}, discount={discount_w}")

        for q in TEST_QUERIES:
            candidate_pool = prepare_label(
                bm25_candidates(q, top_n=TOP_N_CANDIDATES)
            )

            hybrid_res = prepare_label(
                balanced_hybrid_search(
                    query=q,
                    top_n_candidates=TOP_N_CANDIDATES,
                    top_k_results=TOP_K,
                    min_umkm_ratio=MIN_UMKM_RATIO,
                    lambda_umkm=LAMBDA_UMKM,
                    alpha=ALPHA,
                    beta=BETA,
                    gamma=GAMMA,
                    popularity_sold_weight=fixed_pop_sold_w,
                    popularity_review_weight=fixed_pop_review_w,
                    value_rating_weight=rating_w,
                    value_discount_weight=discount_w,
                )
            )

            rows.append({
                "query": q,
                "experiment_type": "value_score",
                "sold_weight": fixed_pop_sold_w,
                "review_weight": fixed_pop_review_w,
                "rating_weight": rating_w,
                "discount_weight": discount_w,
                "Precision@40": precision_at_k(hybrid_res, q, TOP_K),
                "Recall@40": recall_at_k(hybrid_res, candidate_pool, q, TOP_K),
                "NDCG@40": ndcg_at_k(hybrid_res, TOP_K),
                "Fairness@40": fairness_at_k(hybrid_res, TOP_K),
                "ExposureDisparity@40": exposure_disparity_at_k(hybrid_res, TOP_K),
            })

    return pd.DataFrame(rows)


# =========================================================
# RINGKASAN HASIL
# =========================================================
def build_summary(df_raw, group_cols, metric_cols):
    summary = (
        df_raw
        .groupby(group_cols)[metric_cols]
        .mean()
        .reset_index()
    )
    return summary


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    print("Menjalankan sensitivity test untuk popularity...")
    df_pop_raw = run_popularity_sensitivity()
    df_pop_summary = build_summary(
        df_pop_raw,
        ["experiment_type", "sold_weight", "review_weight"],
        ["Precision@40", "Recall@40", "NDCG@40", "Fairness@40", "ExposureDisparity@40"]
    )

    print("\nMenjalankan sensitivity test untuk value_score...")
    df_val_raw = run_value_sensitivity()
    df_val_summary = build_summary(
        df_val_raw,
        ["experiment_type", "rating_weight", "discount_weight"],
        ["Precision@40", "Recall@40", "NDCG@40", "Fairness@40", "ExposureDisparity@40"]
    )

    print("\n===== POPULARITY SENSITIVITY SUMMARY =====")
    print(df_pop_summary)

    print("\n===== VALUE_SCORE SENSITIVITY SUMMARY =====")
    print(df_val_summary)

    # Simpan hasil
    df_pop_raw.to_csv("sensitivity_popularity_raw.csv", index=False)
    df_pop_summary.to_csv("sensitivity_popularity_summary.csv", index=False)

    df_val_raw.to_csv("sensitivity_value_raw.csv", index=False)
    df_val_summary.to_csv("sensitivity_value_summary.csv", index=False)

    print("\nFile tersimpan:")
    print("- sensitivity_popularity_raw.csv")
    print("- sensitivity_popularity_summary.csv")
    print("- sensitivity_value_raw.csv")
    print("- sensitivity_value_summary.csv")