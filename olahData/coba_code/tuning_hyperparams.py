import numpy as np
import pandas as pd
import re
from olahData.retrieval.bm25 import bm25_search, bm25_candidates
from olahData.reranking.hybrid_rerank import balanced_hybrid_search


# =========================================================
# METRIC FUNCTIONS
# =========================================================
def relevant_mask(series, query, threshold=0.5):
    q_tokens = re.findall(r'\w+', query.lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k=20, threshold=0.5):
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return mask.mean()


def recall_at_k(df_topk, df_all, query, k=20, threshold=0.5):
    all_rel = relevant_mask(df_all["name_clean"], query, threshold)
    total_relevant = all_rel.sum()

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return topk_rel.sum() / total_relevant


def fairness_at_k(df, k=20):
    return df.head(k)["umkm_label"].mean()


def ndcg_at_k(df, k=20):
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return dcg / idcg if idcg > 0 else 0


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

    return abs(umkm_exp - non_umkm_exp)


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
# CONFIG
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
TOP_K = 20
MIN_UMKM_RATIO = 0.4

# Kombinasi bobot dasar (jumlah = 1)
WEIGHT_CANDIDATES = [
    (0.6, 0.2, 0.2),
    (0.5, 0.25, 0.25),
    (0.5, 0.3, 0.2),
    (0.5, 0.2, 0.3),
    (0.4, 0.3, 0.3),
]

# LAMBDA_VALUES = [0.0, 0.1, 0.2, 0.3, 0.4]
LAMBDA_VALUES = [0.1]



# =========================================================
# BASELINE BM25
# =========================================================
def evaluate_bm25_baseline():
    rows = []

    for q in TEST_QUERIES:
        candidate_pool = prepare_label(bm25_candidates(q, top_n=TOP_N_CANDIDATES))
        bm25_res = prepare_label(bm25_search(q, topk=TOP_K))

        rows.append({
            "query": q,
            "method": "BM25",
            "alpha": None,
            "beta": None,
            "gamma": None,
            "lambda": None,
            "Precision": precision_at_k(bm25_res, q, TOP_K),
            "Recall": recall_at_k(bm25_res, candidate_pool, q, TOP_K),
            "NDCG": ndcg_at_k(bm25_res, TOP_K),
            "Fairness": fairness_at_k(bm25_res, TOP_K),
            "ExposureDisparity": exposure_disparity_at_k(bm25_res, TOP_K),
        })

    return pd.DataFrame(rows)


# =========================================================
# HYBRID GRID SEARCH
# =========================================================
def evaluate_hybrid_grid():
    rows = []

    for q in TEST_QUERIES:
        print(f"\n=== QUERY: {q} ===")
        candidate_pool = prepare_label(bm25_candidates(q, top_n=TOP_N_CANDIDATES))

        for alpha, beta, gamma in WEIGHT_CANDIDATES:
            for lam in LAMBDA_VALUES:
                hybrid_res = prepare_label(
                    balanced_hybrid_search(
                        query=q,
                        top_n_candidates=TOP_N_CANDIDATES,
                        top_k_results=TOP_K,
                        min_umkm_ratio=MIN_UMKM_RATIO,
                        lambda_umkm=lam,
                        alpha=alpha,
                        beta=beta,
                        gamma=gamma
                    )
                )

                rows.append({
                    "query": q,
                    "method": "Hybrid",
                    "alpha": alpha,
                    "beta": beta,
                    "gamma": gamma,
                    "lambda": lam,
                    "Precision": precision_at_k(hybrid_res, q, TOP_K),
                    "Recall": recall_at_k(hybrid_res, candidate_pool, q, TOP_K),
                    "NDCG": ndcg_at_k(hybrid_res, TOP_K),
                    "Fairness": fairness_at_k(hybrid_res, TOP_K),
                    "ExposureDisparity": exposure_disparity_at_k(hybrid_res, TOP_K),
                })

    return pd.DataFrame(rows)


# =========================================================
# SELECT BEST COMBINATION
# =========================================================
def select_best_combination(df_baseline, df_hybrid):
    baseline_mean = df_baseline[["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]].mean()

    precision_floor = baseline_mean["Precision"] - 0.05
    ndcg_floor = baseline_mean["NDCG"] - 0.03

    summary = (
        df_hybrid
        .groupby(["alpha", "beta", "gamma", "lambda"])[
            ["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]
        ]
        .mean()
        .reset_index()
    )

    # kandidat yang relevance-nya masih aman
    eligible = summary[
        (summary["Precision"] >= precision_floor) &
        (summary["NDCG"] >= ndcg_floor)
    ].copy()

    # kalau terlalu ketat dan kosong, fallback ke semua
    if eligible.empty:
        eligible = summary.copy()

    # urutan pemilihan:
    # 1. fairness tertinggi
    # 2. exposure disparity terendah
    # 3. ndcg tertinggi
    eligible = eligible.sort_values(
        by=["Fairness", "ExposureDisparity", "NDCG"],
        ascending=[False, True, False]
    )

    best = eligible.iloc[0]

    return summary, eligible, best, baseline_mean


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    print("Menjalankan baseline BM25...")
    df_baseline = evaluate_bm25_baseline()
    # df_baseline.to_csv("baseline_bm25_tuning.csv", index=False)

    print("Menjalankan grid search hybrid...")
    df_hybrid = evaluate_hybrid_grid()
    # df_hybrid.to_csv("gridsearch_hybrid_results.csv", index=False)

    print("Memilih kombinasi terbaik...")
    summary, eligible, best, baseline_mean = select_best_combination(df_baseline, df_hybrid)

    # summary.to_csv("gridsearch_hybrid_summary.csv", index=False)
    # eligible.to_csv("gridsearch_hybrid_eligible.csv", index=False)

    print("\n===== BASELINE BM25 (RATA-RATA) =====")
    print(baseline_mean)

    print("\n===== RINGKASAN GRID SEARCH =====")
    print(summary)

    print("\n===== KANDIDAT YANG LOLOS BATAS RELEVANCE =====")
    print(eligible)

    print("\n===== KOMBINASI TERBAIK =====")
    print(best)

    # simpan best ke csv satu baris
    pd.DataFrame([best]).to_csv("best_hybrid_params.csv", index=False)