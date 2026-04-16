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
    Menentukan apakah item dianggap relevan terhadap query.
    Relevan jika proporsi token query yang cocok >= threshold.
    """
    q_tokens = re.findall(r"\w+", query.lower())

    def score(text):
        text = str(text).lower()
        match_count = sum(token in text for token in q_tokens)
        return match_count / len(q_tokens) if len(q_tokens) > 0 else 0

    return series.apply(score) >= threshold


def precision_at_k(df, query, k, threshold=0.5):
    """
    Precision@K:
    proporsi item relevan dalam top-K.
    """
    mask = relevant_mask(df.head(k)["name_clean"], query, threshold)
    return float(mask.mean())


def recall_at_k(df_topk, df_all, query, k, threshold=0.5):
    """
    Recall@K:
    proporsi item relevan yang berhasil masuk top-K
    dibanding seluruh item relevan di candidate pool.
    """
    all_rel = relevant_mask(df_all["name_clean"], query, threshold)
    total_relevant = int(all_rel.sum())

    if total_relevant == 0:
        return 0.0

    topk_rel = relevant_mask(df_topk.head(k)["name_clean"], query, threshold)
    return float(topk_rel.sum() / total_relevant)


def fairness_at_k(df, k):
    """
    Fairness@K:
    proporsi produk UMKM dalam top-K.
    """
    return float(df.head(k)["umkm_label"].mean())


def ndcg_at_k(df, k):
    """
    NDCG@K:
    menilai kualitas urutan ranking berdasarkan bm25_score.
    """
    df_k = df.head(k).copy()
    df_k["rel"] = df_k["bm25_score"]

    dcg = np.sum(df_k["rel"] / np.log2(np.arange(2, len(df_k) + 2)))
    ideal = df_k.sort_values("rel", ascending=False)
    idcg = np.sum(ideal["rel"] / np.log2(np.arange(2, len(ideal) + 2)))

    return float(dcg / idcg) if idcg > 0 else 0.0


def exposure_disparity_at_k(df, k):
    """
    Exposure Disparity@K:
    selisih rata-rata exposure UMKM vs non-UMKM.
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
    Mengubah label UMKM agar konsisten menjadi angka:
    UMKM = 1, NON_UMKM = 0
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
# 2. KONFIGURASI EKSPERIMEN
# =========================================================

TEST_QUERIES = [
    "kopi khas daerah",
    "kopi instan sachet",
    "baju batik pria",
    "tas wanita kulit",
    "keripik singkong",
    "hiasan rumah handmade",
]

# Nilai K yang ingin diuji
K_VALUES = [20, 30, 40, 50, 60, 70, 80, 90, 100]

# Candidate pool awal dari BM25
TOP_N_CANDIDATES = 2000

# Minimal proporsi UMKM di hasil hybrid
MIN_UMKM_RATIO = 0.4

# Parameter tetap sementara untuk Stage 1
ALPHA = 0.5
BETA = 0.25
GAMMA = 0.25
LAMBDA_UMKM = 0.3


# =========================================================
# 3. EVALUASI UNTUK SETIAP NILAI K
# =========================================================

def evaluate_for_k(k):
    """
    Menghitung metrik BM25 dan Hybrid untuk satu nilai K.
    """
    rows = []

    for q in TEST_QUERIES:
        print(f"Evaluasi query='{q}' | K={k}")

        candidate_pool = prepare_label(
            bm25_candidates(q, top_n=TOP_N_CANDIDATES)
        )

        # -----------------------------
        # BM25 baseline
        # -----------------------------
        bm25_res = prepare_label(
            bm25_search(q, topk=k)
        )

        rows.append({
            "query": q,
            "method": "BM25",
            "K": k,
            "Precision": precision_at_k(bm25_res, q, k),
            "Recall": recall_at_k(bm25_res, candidate_pool, q, k),
            "NDCG": ndcg_at_k(bm25_res, k),
            "Fairness": fairness_at_k(bm25_res, k),
            "ExposureDisparity": exposure_disparity_at_k(bm25_res, k),
        })

        # -----------------------------
        # Hybrid fairness-aware
        # -----------------------------
        hybrid_res = prepare_label(
            balanced_hybrid_search(
                query=q,
                top_n_candidates=TOP_N_CANDIDATES,
                top_k_results=k,
                min_umkm_ratio=MIN_UMKM_RATIO,
                lambda_umkm=LAMBDA_UMKM,
                alpha=ALPHA,
                beta=BETA,
                gamma=GAMMA
            )
        )

        rows.append({
            "query": q,
            "method": "Hybrid",
            "K": k,
            "Precision": precision_at_k(hybrid_res, q, k),
            "Recall": recall_at_k(hybrid_res, candidate_pool, q, k),
            "NDCG": ndcg_at_k(hybrid_res, k),
            "Fairness": fairness_at_k(hybrid_res, k),
            "ExposureDisparity": exposure_disparity_at_k(hybrid_res, k),
        })

    return pd.DataFrame(rows)


# =========================================================
# 4. MEMBUAT SUMMARY, COMPARISON, DAN RANKED
# =========================================================

def build_stage1_outputs(df_results):
    """
    Menghasilkan 4 output utama:
    1. summary    -> rata-rata BM25 dan Hybrid per K
    2. comparison -> BM25 vs Hybrid per K dalam 1 tabel
    3. ranked     -> SEMUA nilai K yang sudah diurutkan
    """

    # =====================================================
    # SUMMARY
    # =====================================================
    summary = (
        df_results
        .groupby(["method", "K"])[["Precision", "Recall", "NDCG", "Fairness", "ExposureDisparity"]]
        .mean()
        .reset_index()
    )

    # =====================================================
    # COMPARISON
    # Gabungkan BM25 dan Hybrid agar mudah dibandingkan
    # =====================================================
    bm25_summary = summary[summary["method"] == "BM25"].copy()
    hybrid_summary = summary[summary["method"] == "Hybrid"].copy()

    comparison = hybrid_summary.merge(
        bm25_summary,
        on="K",
        suffixes=("_hybrid", "_bm25")
    )

    # =====================================================
    # RANKED
    # =====================================================
    ranked = comparison.copy()

    # hitung gap relevance terhadap BM25
    ranked["precision_gap"] = ranked["Precision_bm25"] - ranked["Precision_hybrid"]
    ranked["ndcg_gap"] = ranked["NDCG_bm25"] - ranked["NDCG_hybrid"]

    # urutkan secara bertahap
    ranked = ranked.sort_values(
        by=[
            "precision_gap",               # gap precision paling kecil
            "ndcg_gap",                    # gap ndcg paling kecil
            "Fairness_hybrid",             # fairness terbesar
            "ExposureDisparity_hybrid",    # exposure disparity terkecil
            "K"                            # K lebih kecil diutamakan jika seri
        ],
        ascending=[True, True, False, True, True]
    ).reset_index(drop=True)

    ranked["rank_k"] = range(1, len(ranked) + 1)

    return summary, comparison, ranked


# =========================================================
# 5. MAIN
# =========================================================

if __name__ == "__main__":
    all_results = []

    # Jalankan evaluasi untuk setiap K
    for k in K_VALUES:
        df_k = evaluate_for_k(k)
        all_results.append(df_k)

    df_results = pd.concat(all_results, ignore_index=True)

    # Bentuk 4 output utama
    summary, comparison, ranked = build_stage1_outputs(df_results)

    # Simpan hanya 4 file utama
    summary.to_csv("stage1_k_summary.csv", index=False)
    comparison.to_csv("stage1_k_comparison.csv", index=False)
    ranked.to_csv("stage1_k_ranked.csv", index=False)

    # Tampilkan ke terminal
    print("\n===== SUMMARY =====")
    print(summary)

    print("\n===== COMPARISON =====")
    print(comparison)

    print("\n===== RANKED =====")
    print(ranked)