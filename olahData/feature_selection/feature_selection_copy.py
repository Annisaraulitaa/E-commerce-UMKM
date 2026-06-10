"""
FEATURE SELECTION — Hybrid Re-ranking UMKM

Tujuan:
1. Membuat kombinasi fitur untuk komponen beta/popularity dan gamma/value.
2. Menyimpan mapping fitur untuk alpha, beta, gamma, lambda.

Input yang disarankan: dataset hasil preprocessing, misalnya:
- countSold, countReview, totalRating sudah numerik
- ratingAverage, discountPercentage sudah numerik
- umkm_score / umkm_label sudah tersedia
- bm25_score / bm25_norm tersedia jika dataset sudah digabung dengan hasil BM25

Cara menjalankan:
python feature_selection.py --input "D:/Kuliah/DATA_TA/olahData/retrieval/nondup_labeled_dataset(new).csv" 
"""

import argparse
import itertools
import os
from typing import List

import numpy as np
import pandas as pd


# ============================================================
# Konfigurasi kandidat fitur per komponen
# ============================================================
POPULARITY_RAW = ["countSold", "countReview", "totalRating", "price_number"]
VALUE_RAW = ["ratingAverage", "discountPercentage"]
UMKM_CANDIDATES = ["umkm_label"]
ALPHA_CANDIDATES = ["bm25_score"]


# ============================================================
# Helper umum
# ============================================================
def to_numeric_safe(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(float)


def minmax(series: pd.Series) -> pd.Series:
    s = to_numeric_safe(series)
    min_val, max_val = s.min(), s.max()
    if max_val == min_val:
        return pd.Series(0.0, index=series.index)
    return (s - min_val) / (max_val - min_val)


def available_columns(df: pd.DataFrame, cols: List[str]) -> List[str]:
    return [col for col in cols if col in df.columns]


def get_norm_col(df: pd.DataFrame, raw_col: str, log_transform: bool = False) -> str:
    norm_col = f"norm_{raw_col}"

    if norm_col in df.columns:
        return norm_col

    values = to_numeric_safe(df[raw_col])
    if log_transform:
        values = np.log1p(values)

    df[norm_col] = minmax(values)
    return norm_col


def make_component_combinations(
    df: pd.DataFrame,
    norm_features: List[str],
    component_name: str,
    output_dir: str,
) -> pd.DataFrame:
    rows = []

    for size in range(1, len(norm_features) + 1):
        for combo in itertools.combinations(norm_features, size):
            raw_names = [col.replace("norm_", "") for col in combo]
            score_col = f"{component_name}_score__" + "__".join(raw_names)

            df[score_col] = df[list(combo)].mean(axis=1)

            rows.append({
                "component": component_name,
                "features": " + ".join(raw_names),
                "n_features": len(combo),
                "score_column": score_col,
                "mean": df[score_col].mean(),
                "std": df[score_col].std(),
                "min": df[score_col].min(),
                "max": df[score_col].max(),
                "zero_pct": (df[score_col] == 0).mean() * 100,
            })

    result = pd.DataFrame(rows)
    result_path = os.path.join(output_dir, f"{component_name}_ablation_combinations.csv")
    result.to_csv(result_path, index=False)

    return result


def report_raw_stats(
    df: pd.DataFrame,
    features: List[str],
    output_dir: str,
    name: str,
) -> pd.DataFrame:
    rows = []

    for col in features:
        s = to_numeric_safe(df[col])

        rows.append({
            "feature": col,
            "mean": s.mean(),
            "std": s.std(),
            "min": s.min(),
            "max": s.max(),
            "zero_pct": (s == 0).mean() * 100,
            "null_pct": df[col].isnull().mean() * 100,
        })

    result = pd.DataFrame(rows)
    result.to_csv(
        os.path.join(output_dir, f"{name}_raw_stats.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    return result


def feature_correlation(
    df: pd.DataFrame,
    features: List[str],
    output_dir: str,
) -> pd.DataFrame:
    numeric_df = df[features].apply(to_numeric_safe)
    corr = numeric_df.corr()

    corr.to_csv(
        os.path.join(output_dir, "feature_correlation.csv"),
        encoding="utf-8-sig",
    )

    return corr


def build_alpha_feature(df: pd.DataFrame) -> str:
    if "bm25_score" in df.columns:
        df["bm25_norm"] = minmax(df["bm25_score"])
        return "bm25_score -> bm25_norm"

    return "bm25_score dari output BM25/candidate retrieval"


def build_umkm_boost(df: pd.DataFrame) -> str:
    df["umkm_boost"] = (
        df["umkm_label"]
        .astype(str)
        .str.upper()
        .str.strip()
        .eq("UMKM")
        .astype(int)
    )
    return "umkm_boost"


def validate_columns(df: pd.DataFrame) -> None:
    required = POPULARITY_RAW + VALUE_RAW + ["umkm_label"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Kolom berikut tidak ditemukan: {missing}")


# ============================================================
# Main
# ============================================================
def run_feature_selection(input_path: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_path, low_memory=False)
    validate_columns(df)
    print(f"Dataset dimuat: {df.shape[0]:,} baris × {df.shape[1]:,} kolom")

    # ---------- Laporan awal fitur mentah ----------
    report_raw_stats(df, POPULARITY_RAW, output_dir, "popularity")
    report_raw_stats(df, VALUE_RAW, output_dir, "value")
    feature_correlation(df, POPULARITY_RAW + VALUE_RAW, output_dir)

    # ---------- Alpha ----------
    alpha_feature = build_alpha_feature(df)

    # ---------- Beta / Popularity ----------
    popularity_available = available_columns(df, POPULARITY_RAW)
    if not popularity_available:
        raise ValueError("Tidak ada fitur popularity yang ditemukan.")

    popularity_norm = [
        get_norm_col(df, col, log_transform=True)
        for col in popularity_available
    ]
    popularity_combos = make_component_combinations(
        df=df,
        norm_features=popularity_norm,
        component_name="popularity",
        output_dir=output_dir,
    )

    # ---------- Gamma / Value ----------
    value_available = available_columns(df, VALUE_RAW)
    if not value_available:
        raise ValueError("Tidak ada fitur value yang ditemukan.")

    value_norm = [
        get_norm_col(df, col, log_transform=False)
        for col in value_available
    ]
    value_combos = make_component_combinations(
        df=df,
        norm_features=value_norm,
        component_name="value",
        output_dir=output_dir,
    )

    # ---------- Lambda / UMKM Boost ----------
    umkm_feature = build_umkm_boost(df)

    # ---------- Mapping komponen ----------
    mapping = pd.DataFrame([
        {
            "parameter": "alpha",
            "component": "BM25 relevance",
            "feature_source": alpha_feature,
            "note": "Alpha memakai skor BM25. Kolom teks bukan fitur alpha langsung.",
        },
        {
            "parameter": "beta",
            "component": "Popularity",
            "feature_source": " + ".join(popularity_available),
            "note": "Diuji melalui semua kombinasi fitur popularity.",
        },
        {
            "parameter": "gamma",
            "component": "Value",
            "feature_source": " + ".join(value_available),
            "note": "Diuji melalui semua kombinasi fitur value.",
        },
        {
            "parameter": "lambda",
            "component": "UMKM Boost",
            "feature_source": umkm_feature,
            "note": "Menggunakan skor/label UMKM hasil preprocessing atau rule-based tagging sebelumnya.",
        },
    ])
    mapping.to_csv(os.path.join(output_dir, "feature_component_mapping.csv"), index=False)

    # ---------- Simpan dataset ringkas ----------
    id_cols = available_columns(df, [
        "id", "name", "category_breadcrumb",
        "shop_name", "shop_city", "url"
    ])

    score_cols = []
    if "bm25_norm" in df.columns:
        score_cols.append("bm25_norm")
    elif "bm25_score" in df.columns:
        score_cols.append("bm25_score")

    score_cols += list(popularity_combos["score_column"])
    score_cols += list(value_combos["score_column"])
    score_cols += [umkm_feature]

    raw_cols = available_columns(df, POPULARITY_RAW + VALUE_RAW + UMKM_CANDIDATES)
    selected_cols = list(dict.fromkeys(id_cols + raw_cols + score_cols))

    df[selected_cols].to_csv(
        os.path.join(output_dir, "dataset_feature_selection.csv"),
        index=False,
    )

    print("\nFeature selection selesai.")
    print(f"Output folder: {output_dir}")
    print("- popularity_raw_stats.csv")
    print("- value_raw_stats.csv")
    print("- feature_correlation.csv")
    print("- popularity_ablation_combinations.csv")
    print("- value_ablation_combinations.csv")
    print("- feature_component_mapping.csv")
    print("- dataset_feature_selection.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path dataset hasil preprocessing")
    parser.add_argument("--output", default="output_feature_selection")
    args = parser.parse_args()

    run_feature_selection(args.input, args.output)
