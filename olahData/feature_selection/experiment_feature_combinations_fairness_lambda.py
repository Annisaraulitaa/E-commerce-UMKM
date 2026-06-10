"""
FAIRNESS LAMBDA EXPERIMENT — Hybrid Ranking UMKM

- Menguji beberapa nilai lambda/fairness boost setelah kombinasi fitur dan skenario dasar terbaik ditemukan
- Menggunakan 1 kombinasi fitur popularity, 1 kombinasi fitur value, dan 1 skenario bobot dasar
- Menghasilkan ranking Top-K dan ringkasan metrik evaluasi

Rumus:
score_base  = alpha * bm25
            + beta  * popularity_score
            + gamma * value_score

score_final = score_base + lambda * umkm_boost
final_score = score_final

Catatan:
- alpha + beta + gamma harus = 1.00
- lambda tidak dimasukkan ke total bobot dasar, karena lambda digunakan sebagai boost tambahan
- Ubah SELECTED_POPULARITY_ID, SELECTED_VALUE_ID, SELECTED_BASE_SCENARIO, dan FAIRNESS_LAMBDAS sesuai hasil eksperimen sebelumnya
"""

from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# KONFIGURASI
# ============================================================

INPUT_CSV = "manual_labeling_top30_labeled.csv"
OUTPUT_DIR = "output_experiment_fairness_lambda"

K_VALUES = [5, 10, 15, 20, 25]


# ============================================================
# FITUR DAN SKENARIO TERPILIH
# ============================================================

SELECTED_POPULARITY_ID = "B5_countSold_totalRating"
SELECTED_POPULARITY_FEATURES = ["countSold", "totalRating"]

SELECTED_VALUE_ID = "G3_ratingAverage_discountPercentage"
SELECTED_VALUE_FEATURES = ["ratingAverage", "discountPercentage"]

SELECTED_BASE_SCENARIO = "relevance_dominant_no_fairness"
BASE_WEIGHT = {
    "alpha": 0.50,
    "beta": 0.25,
    "gamma": 0.25,
}

FAIRNESS_LAMBDAS = [0.10, 0.20, 0.30, 0.40, 0.50]


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


def check_config() -> None:
    base_total = BASE_WEIGHT["alpha"] + BASE_WEIGHT["beta"] + BASE_WEIGHT["gamma"]

    if not np.isclose(base_total, 1.0):
        raise ValueError(
            f"Total bobot dasar {SELECTED_BASE_SCENARIO} tidak sama dengan 1: {base_total}"
        )

    invalid_lambdas = [v for v in FAIRNESS_LAMBDAS if not (0.0 <= v <= 1.0)]
    if invalid_lambdas:
        raise ValueError(f"Nilai lambda harus berada pada rentang [0, 1]: {invalid_lambdas}")

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

    # alpha = BM25
    if "bm25_norm" in df.columns:
        df["alpha_bm25"] = to_num(df["bm25_norm"]).clip(0, 1)
    else:
        df["alpha_bm25"] = minmax(df["bm25_score"])

    # beta = popularity
    for col in ["countSold", "countReview", "totalRating"]:
        df[f"norm_{col}"] = log_minmax(df[col])

    # gamma = value
    for col in ["ratingAverage", "discountPercentage"]:
        df[f"norm_{col}"] = minmax(df[col])

    # lambda = UMKM boost
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

def confusion_at_k(binary_labels: np.ndarray, total_relevant: int, k: int) -> tuple[int, int, int]:
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
    check_config()

    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    df, sep = read_csv_auto(INPUT_CSV)
    df = prepare_features(df)

    pop_id = SELECTED_POPULARITY_ID
    value_id = SELECTED_VALUE_ID
    scenario = SELECTED_BASE_SCENARIO

    pop_features = SELECTED_POPULARITY_FEATURES
    value_features = SELECTED_VALUE_FEATURES
    base_weight = BASE_WEIGHT

    print(f"Input      : {INPUT_CSV}")
    print(f"Shape      : {df.shape}")
    print(f"Sep        : {repr(sep)}")
    print(f"Output     : {output_dir}")
    print(f"Popularity : {pop_id} -> {' + '.join(pop_features)}")
    print(f"Value      : {value_id} -> {' + '.join(value_features)}")
    print(f"Scenario   : {scenario}")
    print(f"Lambdas    : {FAIRNESS_LAMBDAS}")

    base = df.copy()
    base["popularity_score"] = component_score(base, pop_features)
    base["value_score"] = component_score(base, value_features)
    base["score_base"] = (
        base_weight["alpha"] * base["alpha_bm25"]
        + base_weight["beta"] * base["popularity_score"]
        + base_weight["gamma"] * base["value_score"]
    )

    summary_rows = []
    per_query_rows = []
    topk_rows = []

    for experiment_id, lambda_value in enumerate(FAIRNESS_LAMBDAS, start=1):
        lambda_label = str(lambda_value).replace(".", "p")
        exp_name = f"L{lambda_label}_{pop_id}__{value_id}__{scenario}"

        temp = base.copy()
        temp["score_final"] = (
            temp["score_base"] + lambda_value * temp["lambda_umkm"]
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
                "popularity_id": pop_id,
                "popularity_features": " + ".join(pop_features),
                "value_id": value_id,
                "value_features": " + ".join(value_features),
                "scenario": scenario,
                "alpha": base_weight["alpha"],
                "beta": base_weight["beta"],
                "gamma": base_weight["gamma"],
                "lambda": lambda_value,
                **metrics,
            }

            per_query_rows.append(row)
            query_metric_rows.append(metrics)

            top = ranked.head(max(K_VALUES)).copy()
            top["experiment_id"] = experiment_id
            top["experiment_name"] = exp_name
            top["scenario"] = scenario
            top["lambda"] = lambda_value
            top["rank"] = np.arange(1, len(top) + 1)

            keep_cols = [
                "experiment_id",
                "experiment_name",
                "scenario",
                "lambda",
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
            "value_id": value_id,
            "value_features": " + ".join(value_features),
            "scenario": scenario,
            "alpha": base_weight["alpha"],
            "beta": base_weight["beta"],
            "gamma": base_weight["gamma"],
            "lambda": lambda_value,
        }
        for col in metrics_df.columns:
            summary[f"mean_{col}"] = metrics_df[col].mean()

        summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)
    per_query_df = pd.DataFrame(per_query_rows)
    topk_df = pd.concat(topk_rows, ignore_index=True)

    summary_df.to_csv(
        output_dir / "fairness_lambda_experiment_summary.csv",
        index=False,
        sep=sep,
        encoding="utf-8-sig",
    )
    per_query_df.to_csv(
        output_dir / "per_query_metrics.csv",
        index=False,
        sep=sep,
        encoding="utf-8-sig",
    )
    topk_df.to_csv(
        output_dir / "topk_rankings.csv",
        index=False,
        sep=sep,
        encoding="utf-8-sig",
    )

    print("\nSelesai.")
    print("Output utama:")
    print(output_dir / "fairness_lambda_experiment_summary.csv")
    print(output_dir / "per_query_metrics.csv")
    print(output_dir / "topk_rankings.csv")


if __name__ == "__main__":
    run_experiment()
