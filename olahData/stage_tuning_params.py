import numpy as np
import pandas as pd
import re
from retrieval.bm25 import bm25_search, bm25_candidates
from reranking.hybrid_rerank import balanced_hybrid_search


# =========================================================
# 1. FUNGSI METRIK
# =========================================================

def relevant_mask(series, query, threshold=0.5):
    """
    Menentukan apakah item relevan terhadap query.
    """
    q_tokens = re.findall(r"\w+", query.lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k, threshold=0.5):
    """
    Precision@K = proporsi item relevan dalam top-K.
    """
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return float(mask.mean())


def recall_at_k(df_topk, df_all, query, k, threshold=0.5):
    """
    Recall@K = proporsi item relevan yang berhasil masuk top-K
    dibanding semua item relevan dalam candidate pool.
    """
    all_rel = relevant_mask(df_all["name_clean"], query, threshold)
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return float(topk_rel.sum() / total_relevant)


def fairness_at_k(df, k):
    """
    Fairness@K = proporsi UMKM dalam top-K.
    """
    return float(df.head(k)["umkm_label"].mean())


def ndcg_at_k(df, k):
    """
    NDCG@K = kualitas urutan ranking.
    """
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return float(dcg / idcg) if idcg > 0 else 0.0


def exposure_disparity_at_k(df, k):
    """
    Exposure Disparity@K = selisih rata-rata exposure UMKM vs non-UMKM.
    Semakin kecil, semakin baik.
    """
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
    """
    Menyamakan label UMKM menjadi numerik.
    """
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
# 2. KONFIGURASI
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
MIN_UMKM_RATIO = 0.4

# Kombinasi bobot dasar
# Catatan:
# - saya batasi kombinasi agar eksperimen tidak terlalu berat
# - fokus pada relevance dominan, popularity/value sebagai pendukung
WEIGHT_CANDIDATES = [
    (0.4, 0.30, 0.30),
    (0.4, 0.25, 0.35),
    (0.4, 0.35, 0.25),
    (0.5, 0.25, 0.25),
    (0.5, 0.30, 0.20),
    (0.5, 0.20, 0.30),
    (0.6, 0.20, 0.20),
    (0.6, 0.25, 0.15),
    (0.6, 0.15, 0.25),
]

LAMBDA_VALUES = [0.0, 0.1, 0.2, 0.3, 0.4]


# =========================================================
# 3. AMBIL TOP 5 NILAI K DARI HASIL STAGE 1
# =========================================================

def load_top5_k():
    """
    Membaca 5 nilai K teratas dari stage1_k_ranked.csv
    berdasarkan urutan rank_k.
    """
    df_ranked = pd.read_csv("stage1_k_ranked.csv")
    top5_k = df_ranked.sort_values("rank_k").head(5)["K"].tolist()
    return top5_k


# =========================================================
# 4. EVALUASI BASELINE BM25
# =========================================================

def evaluate_bm25_for_k(k):
    """
    Evaluasi BM25 baseline untuk satu nilai K.
    Hasil dikembalikan per query, lalu nanti dirata-ratakan.
    """
    rows = []

    for q in TEST_QUERIES:
        candidate_pool = prepare_label(
            bm25_candidates(q, top_n=TOP_N_CANDIDATES)
        )

        bm25_res = prepare_label(
            bm25_search(q, topk=k)
        )

        rows.append({
            "query": q,
            "K": k,
            "method": "BM25",
            "alpha": None,
            "beta": None,
            "gamma": None,
            "lambda": None,
            "Precision": precision_at_k(bm25_res, q, k),
            "Recall": recall_at_k(bm25_res, candidate_pool, q, k),
            "NDCG": ndcg_at_k(bm25_res, k),
            "Fairness": fairness_at_k(bm25_res, k),
            "ExposureDisparity": exposure_disparity_at_k(bm25_res, k),
        })

    return pd.DataFrame(rows)


# =========================================================
# 5. EVALUASI HYBRID UNTUK SEMUA KOMBINASI PARAMETER
# =========================================================

def evaluate_hybrid_for_k(k):
    """
    Evaluasi hybrid untuk satu nilai K dan semua kombinasi parameter.
    Hasil dikembalikan per query, lalu nanti dirata-ratakan.
    """
    rows = []

    for q in TEST_QUERIES:
        candidate_pool = prepare_label(
            bm25_candidates(q, top_n=TOP_N_CANDIDATES)
        )

        for alpha, beta, gamma in WEIGHT_CANDIDATES:
            for lam in LAMBDA_VALUES:
                hybrid_res = prepare_label(
                    balanced_hybrid_search(
                        query=q,
                        top_n_candidates=TOP_N_CANDIDATES,
                        top_k_results=k,
                        min_umkm_ratio=MIN_UMKM_RATIO,
                        lambda_umkm=lam,
                        alpha=alpha,
                        beta=beta,
                        gamma=gamma
                    )
                )

                rows.append({
                    "query": q,
                    "K": k,
                    "method": "Hybrid",
                    "alpha": alpha,
                    "beta": beta,
                    "gamma": gamma,
                    "lambda": lam,
                    "Precision": precision_at_k(hybrid_res, q, k),
                    "Recall": recall_at_k(hybrid_res, candidate_pool, q, k),
                    "NDCG": ndcg_at_k(hybrid_res, k),
                    "Fairness": fairness_at_k(hybrid_res, k),
                    "ExposureDisparity": exposure_disparity_at_k(hybrid_res, k),
                })

    return pd.DataFrame(rows)


# =========================================================
# 6. BANGUN OUTPUT SUMMARY, COMPARISON, ELIGIBLE, RANKED
# =========================================================

def build_stage2_outputs(df_baseline_all, df_hybrid_all):
    """
    Menghasilkan 4 output akhir Stage 2:
    1. summary
    2. comparison
    3. eligible
    4. ranked
    """

    # -----------------------------------------------------
    # SUMMARY
    # Ringkasan rata-rata semua kombinasi parameter hybrid
    # -----------------------------------------------------
    summary = (
        df_hybrid_all
        .groupby(["K", "alpha", "beta", "gamma", "lambda"])[
            ["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]
        ]
        .mean()
        .reset_index()
    )

    # -----------------------------------------------------
    # BASELINE MEAN PER K
    # Dipakai sebagai acuan pembanding
    # -----------------------------------------------------
    baseline_mean = (
        df_baseline_all
        .groupby("K")[["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]]
        .mean()
        .reset_index()
    )

    baseline_mean = baseline_mean.rename(columns={
        "Precision": "Precision_bm25",
        "Recall": "Recall_bm25",
        "NDCG": "NDCG_bm25",
        "Fairness": "Fairness_bm25",
        "ExposureDisparity": "ExposureDisparity_bm25",
    })

    # -----------------------------------------------------
    # COMPARISON
    # Gabungkan hasil hybrid summary dengan baseline BM25 per K
    # -----------------------------------------------------
    comparison = summary.merge(
        baseline_mean,
        on="K",
        how="left"
    )

    # -----------------------------------------------------
    # ELIGIBLE
    # Kandidat kombinasi yang lolos aturan minimum
    # -----------------------------------------------------
    eligible = comparison[
        (comparison["Fairness"] >= 0.40) &
        (comparison["Precision"] >= comparison["Precision_bm25"] - 0.05) &
        (comparison["NDCG"] >= comparison["NDCG_bm25"] - 0.03)
    ].copy()

    # -----------------------------------------------------
    # RANKED
    # Semua kombinasi parameter diurutkan
    # -----------------------------------------------------
    ranked = comparison.copy()

    ranked["is_eligible"] = (
        (ranked["Fairness"] >= 0.40) &
        (ranked["Precision"] >= ranked["Precision_bm25"] - 0.05) &
        (ranked["NDCG"] >= ranked["NDCG_bm25"] - 0.03)
    )

    # Ranking global:
    # 1. yang eligible ditaruh di atas
    # 2. precision tinggi
    # 3. fairness tinggi
    # 4. ndcg tinggi
    # 5. exposure disparity rendah
    ranked = ranked.sort_values(
        by=[
            "is_eligible",
            "Precision",
            "Fairness",
            "NDCG",
            "ExposureDisparity"
        ],
        ascending=[False, False, False, False, True]
    ).reset_index(drop=True)

    ranked["rank_param"] = range(1, len(ranked) + 1)

    return summary, comparison, eligible, ranked


# =========================================================
# 7. MAIN
# =========================================================

if __name__ == "__main__":
    top5_k = load_top5_k()
    print("Top 5 K dari Stage 1:", top5_k)

    baseline_frames = []
    hybrid_frames = []

    for k in top5_k:
        print(f"\nEvaluasi Stage 2 untuk K={k}")

        df_bm25_k = evaluate_bm25_for_k(k)
        df_hybrid_k = evaluate_hybrid_for_k(k)

        baseline_frames.append(df_bm25_k)
        hybrid_frames.append(df_hybrid_k)

    df_baseline_all = pd.concat(baseline_frames, ignore_index=True)
    df_hybrid_all = pd.concat(hybrid_frames, ignore_index=True)

    summary, comparison, eligible, ranked = build_stage2_outputs(
        df_baseline_all,
        df_hybrid_all
    )

    # Simpan hanya 4 file utama
    summary.to_csv("stage2_param_summary.csv", index=False)
    comparison.to_csv("stage2_param_comparison.csv", index=False)
    eligible.to_csv("stage2_param_eligible.csv", index=False)
    ranked.to_csv("stage2_param_ranked.csv", index=False)

    print("\n===== STAGE 2 SUMMARY =====")
    print(summary)

    print("\n===== STAGE 2 COMPARISON =====")
    print(comparison)

    print("\n===== STAGE 2 ELIGIBLE =====")
    print(eligible)

    print("\n===== STAGE 2 RANKED =====")
    print(ranked)