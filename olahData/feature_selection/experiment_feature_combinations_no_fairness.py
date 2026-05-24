"""
FULL COMBINATION EXPERIMENT — Base Ranking Feature Selection UMKM

- Menguji semua kombinasi fitur β (popularity) dan γ (value)
- Menguji setiap kombinasi tersebut pada beberapa skenario bobot dasar α, β, γ tanpa fairness boost
- Menghasilkan ranking Top-K dan ringkasan metrik evaluasi

Rumus:
score_base  = alpha * bm25
            + beta  * popularity_score
            + gamma * value_score

score_final = score_base  # lambda = 0.00 pada tahap pemilihan fitur
final_score = score_final
"""

import itertools
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# KONFIGURASI
# ============================================================

INPUT_CSV = "manual_labeling_top30_labeled.csv"
OUTPUT_DIR = "output_experiment_feature_combination_no_fairness"

K_VALUES = [5, 10, 15, 20, 25]

POPULARITY_CANDIDATES = {
    "B1_countSold": ["countSold"],
    "B2_countReview": ["countReview"],
    "B3_totalRating": ["totalRating"],
    "B4_countSold_countReview": ["countSold", "countReview"],
    "B5_countSold_totalRating": ["countSold", "totalRating"],
    "B6_countReview_totalRating": ["countReview", "totalRating"],
    "B7_countSold_countReview_totalRating": ["countSold", "countReview", "totalRating"],
}

VALUE_CANDIDATES = {
    "G1_ratingAverage": ["ratingAverage"],
    "G2_discountPercentage": ["discountPercentage"],
    "G3_ratingAverage_discountPercentage": ["ratingAverage", "discountPercentage"],
}

WEIGHT_SCENARIOS = {
    "balanced_no_fairness": {
        "alpha": 0.34, "beta": 0.33, "gamma": 0.33, "lambda": 0.00,
    },
    "relevance_dominant_no_fairness": {
        "alpha": 0.50, "beta": 0.25, "gamma": 0.25, "lambda": 0.00,
    },
    "popularity_dominant_no_fairness": {
        "alpha": 0.25, "beta": 0.50, "gamma": 0.25, "lambda": 0.00,
    },
    "value_dominant_no_fairness": {
        "alpha": 0.25, "beta": 0.25, "gamma": 0.50, "lambda": 0.00,
    },
}


# ============================================================
# FUNGSI UTILITAS
# ============================================================

def read_csv_auto(path: str) -> tuple[pd.DataFrame, str]:
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        first_line = f.readline()

    sep = ";" if first_line.count(";") > first_line.count(",") else ","
    df = pd.read_csv(path, sep=sep, encoding="utf-8-sig", low_memory=False)

    if df.shape[1] == 1:
        sep = "," if sep == ";" else ";"
        df = pd.read_csv(path, sep=sep, encoding="utf-8-sig", low_memory=False)

    return df, sep


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(float)


def minmax(series: pd.Series) -> pd.Series:
    s = to_num(series)
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(0.0, index=series.index)
    return (s - lo) / (hi - lo)


def log_minmax(series: pd.Series) -> pd.Series:
    s = np.log1p(to_num(series).clip(lower=0))
    return minmax(s)


def check_weights() -> None:
    for name, w in WEIGHT_SCENARIOS.items():
        base_total = w["alpha"] + w["beta"] + w["gamma"]

        if not np.isclose(base_total, 1.0):
            raise ValueError(
                f"Total bobot dasar {name} tidak sama dengan 1: {base_total}"
            )

        if not (0.0 <= w["lambda"] <= 1.0):
            raise ValueError(
                f"Nilai lambda {name} harus berada pada rentang [0, 1]: {w['lambda']}"
            )


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "query",
        "manual_label",
        "countSold",
        "countReview",
        "totalRating",
        "ratingAverage",
        "discountPercentage",
        "umkm_label",
    ]
    missing = [c for c in required if c not in df.columns]

    if "bm25_norm" not in df.columns and "bm25_score" not in df.columns:
        missing.append("bm25_norm / bm25_score")

    if missing:
        raise ValueError(f"Kolom wajib tidak ditemukan: {missing}")

    df = df.copy()

    # α = BM25
    if "bm25_norm" in df.columns:
        df["alpha_bm25"] = to_num(df["bm25_norm"]).clip(0, 1)
    else:
        df["alpha_bm25"] = minmax(df["bm25_score"])

    # β = popularity
    for col in ["countSold", "countReview", "totalRating"]:
        df[f"norm_{col}"] = log_minmax(df[col])

    # γ = value
    for col in ["ratingAverage", "discountPercentage"]:
        df[f"norm_{col}"] = minmax(df[col])

    # λ = UMKM boost
    label_text = df["umkm_label"].astype(str).str.upper().str.strip()
    df["lambda_umkm"] = np.where(label_text.eq("UMKM"), 1.0, 0.0)

    # Relevance: 2 = sangat relevan, 1 = agak relevan, 0 = tidak relevan
    df["_relevance"] = to_num(df["manual_label"]).clip(0, 2)

    print("Distribusi manual_label:", df["_relevance"].value_counts().sort_index().to_dict())

    return df


def component_score(df: pd.DataFrame, features: list[str]) -> pd.Series:
    norm_cols = [f"norm_{f}" for f in features]
    return df[norm_cols].mean(axis=1)


# ============================================================
# METRIK
# ============================================================

def confusion_at_k(
    binary_labels: np.ndarray,
    total_relevant: int,
    k: int,
) -> tuple[int, int, int]:
    """
    Menghitung TP, FP, FN dari top-K hasil ranking.
        TP = jumlah dokumen relevan di top-K
        FP = jumlah dokumen tidak relevan di top-K
        FN = jumlah dokumen relevan yang TIDAK masuk top-K
        FN = total_relevant - TP

    Catatan:
        - binary_labels harus sudah berupa array 0/1 (bukan graded).
        - Pastikan k <= len(binary_labels) agar top-K valid.
        - FN tidak bisa negatif (terjadi jika total_relevant < TP
        karena ketidakkonsistenan data — diproteksi dengan max(0, ...).
    """
    eff_k = min(k, len(binary_labels))
    top_k = binary_labels[:eff_k]

    tp = int(np.sum(top_k == 1))
    fp = int(np.sum(top_k == 0))
    fn = max(0, int(total_relevant - tp))

    return tp, fp, fn


def precision_at_k(binary_labels: np.ndarray, total_relevant: int, k: int) -> float:
    tp, fp, fn = confusion_at_k(binary_labels, total_relevant, k)

    if tp + fp == 0:
        return 0.0

    return float(tp / (tp + fp))


def recall_at_k(binary_labels: np.ndarray, total_relevant: int, k: int) -> float:
    tp, fp, fn = confusion_at_k(binary_labels, total_relevant, k)

    if tp + fn == 0:
        return np.nan

    return float(tp / (tp + fn))


def ndcg_at_k(graded_labels: np.ndarray, k: int) -> float:
    """
    NDCG@K dengan gain standar: gain(rel) = 2^rel - 1

    Formula standar:
        DCG@K  = Σ (2^rel_i - 1) / log2(i + 1)
        IDCG@K = DCG dari ranking ideal
        NDCG@K = DCG@K / IDCG@K
    """
    eff_k = min(k, len(graded_labels))
    top_k = graded_labels[:eff_k]
    ideal = np.sort(graded_labels)[::-1][:eff_k]

    if eff_k == 0 or ideal.sum() == 0:
        return np.nan

    discounts = np.log2(np.arange(2, eff_k + 2))
    gains = 2 ** top_k - 1
    ideal_gains = 2 ** ideal - 1

    dcg = float(np.sum(gains / discounts))
    idcg = float(np.sum(ideal_gains / discounts))

    return float(dcg / idcg) if idcg > 0 else np.nan


def fairness_at_k(umkm_flags: np.ndarray, k: int) -> float:
    eff_k = min(k, len(umkm_flags))

    if eff_k == 0:
        return np.nan

    return float(umkm_flags[:eff_k].sum() / eff_k)


def f1_at_k(binary_labels: np.ndarray, total_relevant: int, k: int) -> float:
    p = precision_at_k(binary_labels, total_relevant, k)
    r = recall_at_k(binary_labels, total_relevant, k)

    if np.isnan(p) or np.isnan(r):
        return np.nan

    if p + r == 0:
        return 0.0

    return float(2 * p * r / (p + r))


def evaluate_group(group: pd.DataFrame) -> dict:
    graded = group["_relevance"].to_numpy(dtype=float)
    binary = (graded >= 1).astype(int)
    umkm = (group["lambda_umkm"].to_numpy(dtype=float) > 0).astype(int)

    total_relevant = int(np.sum(binary))

    metrics = {}

    for k in K_VALUES:
        tp, fp, fn = confusion_at_k(binary, total_relevant, k)

        # Confusion matrix — berguna untuk verifikasi manual
        metrics[f"tp@{k}"] = tp
        metrics[f"fp@{k}"] = fp
        metrics[f"fn@{k}"] = fn

        # Metrik utama
        metrics[f"precision@{k}"] = precision_at_k(binary, total_relevant, k)
        metrics[f"recall@{k}"] = recall_at_k(binary, total_relevant, k)
        metrics[f"ndcg@{k}"] = ndcg_at_k(graded, k)
        metrics[f"fairness@{k}"] = fairness_at_k(umkm, k)
        metrics[f"f1@{k}"] = f1_at_k(binary, total_relevant, k)

    return metrics


# ============================================================
# MAIN EXPERIMENT
# ============================================================

def run_experiment() -> None:
    check_weights()

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    df, sep = read_csv_auto(INPUT_CSV)
    df = prepare_features(df)

    print(f"Input  : {INPUT_CSV}")
    print(f"Shape  : {df.shape}")
    print(f"Sep    : {repr(sep)}")
    print(f"Output : {output_dir}")

    summary_rows = []
    per_query_rows = []
    topk_rows = []

    experiment_id = 0

    feature_pairs = itertools.product(
        POPULARITY_CANDIDATES.items(),
        VALUE_CANDIDATES.items(),
    )

    for (pop_id, pop_features), (val_id, val_features) in feature_pairs:
        base = df.copy()
        base["popularity_score"] = component_score(base, pop_features)
        base["value_score"] = component_score(base, val_features)

        for scenario, w in WEIGHT_SCENARIOS.items():
            experiment_id += 1
            exp_name = f"E{experiment_id:03d}_{pop_id}__{val_id}__{scenario}"

            temp = base.copy()
            temp["score_base"] = (
                w["alpha"] * temp["alpha_bm25"]
                + w["beta"] * temp["popularity_score"]
                + w["gamma"] * temp["value_score"]
            )
            temp["score_final"] = (
                temp["score_base"] + w["lambda"] * temp["lambda_umkm"]
            ).clip(0.0, 1.0)
            temp["final_score"] = temp["score_final"]

            query_metric_rows = []

            for query, group in temp.groupby("query"):
                ranked = group.sort_values("final_score", ascending=False).copy()
                metrics = evaluate_group(ranked)

                row = {
                    "experiment_id": experiment_id,
                    "experiment_name": exp_name,
                    "query": query,
                    "popularity_features": " + ".join(pop_features),
                    "value_features": " + ".join(val_features),
                    "scenario": scenario,
                    "alpha": w["alpha"],
                    "beta": w["beta"],
                    "gamma": w["gamma"],
                    "lambda": w["lambda"],
                    **metrics,
                }

                per_query_rows.append(row)
                query_metric_rows.append(metrics)

                top = ranked.head(max(K_VALUES)).copy()
                top["experiment_id"] = experiment_id
                top["experiment_name"] = exp_name
                top["scenario"] = scenario
                top["rank"] = np.arange(1, len(top) + 1)

                keep_cols = [
                    "experiment_id",
                    "experiment_name",
                    "scenario",
                    "query",
                    "rank",
                    "id",
                    "name",
                    "rank_bm25",
                    "bm25_norm",
                    "score_base",
                    "score_final",
                    "final_score",
                    "popularity_score",
                    "value_score",
                    "lambda_umkm",
                    "manual_label",
                    "_relevance",
                    "umkm_label",
                ]
                keep_cols = [c for c in keep_cols if c in top.columns]
                topk_rows.append(top[keep_cols])

            metrics_df = pd.DataFrame(query_metric_rows)

            summary = {
                "experiment_id": experiment_id,
                "experiment_name": exp_name,
                "popularity_id": pop_id,
                "popularity_features": " + ".join(pop_features),
                "value_id": val_id,
                "value_features": " + ".join(val_features),
                "scenario": scenario,
                "alpha": w["alpha"],
                "beta": w["beta"],
                "gamma": w["gamma"],
                "lambda": w["lambda"],
            }
            for col in metrics_df.columns:
                summary[f"mean_{col}"] = metrics_df[col].mean()

            summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)
    per_query_df = pd.DataFrame(per_query_rows)
    topk_df = pd.concat(topk_rows, ignore_index=True)


    summary_df.to_csv(output_dir / "feature_combination_experiment_summary.csv", index=False, sep=sep, encoding="utf-8-sig")
    per_query_df.to_csv(output_dir / "per_query_metrics.csv", index=False, sep=sep, encoding="utf-8-sig")
    topk_df.to_csv(output_dir / "topk_rankings.csv", index=False, sep=sep, encoding="utf-8-sig")

    print("\nSelesai.")
    print("Output utama:")
    print(output_dir / "feature_combination_experiment_summary.csv")
    print(output_dir / "per_query_metrics.csv")
    print(output_dir / "topk_rankings.csv")


if __name__ == "__main__":
    run_experiment()