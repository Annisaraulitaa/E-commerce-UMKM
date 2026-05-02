# =========================================================
# PLOT TUNING SCENARIO RESULTS
# =========================================================
import os
import pandas as pd
import matplotlib.pyplot as plt


# =========================================================
# FILE INPUT DAN OUTPUT
# =========================================================
SUMMARY_FILE = "joint_tuning_summary_scenario_5metrics.csv"
PLOT_DIR = "scenario_group_plots"

os.makedirs(PLOT_DIR, exist_ok=True)


# =========================================================
# LOAD DATA
# =========================================================
def load_summary(file_path=SUMMARY_FILE):
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File tidak ditemukan: {file_path}\n"
            "Pastikan file summary hasil tuning sudah ada."
        )

    df = pd.read_csv(file_path)

    required_cols = [
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
        "discount_weight",
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "overall_score",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(
            "Kolom berikut tidak ditemukan di summary file:\n"
            f"{missing_cols}"
        )

    return df


# =========================================================
# HELPER: URUTAN SCENARIO NAME
# =========================================================
def get_plot_order(scenario_group, scenario_name):
    order_maps = {
        "relevance_dominant": {
            "rel_45": 1,
            "rel_50": 2,
            "rel_55": 3,
            "rel_60": 4,
            "rel_65": 5,
            "rel_70": 6,
        },
        "popularity_dominant": {
            "pop_45": 1,
            "pop_50": 2,
            "pop_55": 3,
            "pop_60": 4,
            "pop_65": 5,
            "pop_70": 6,
        },
        "value_dominant": {
            "val_45": 1,
            "val_50": 2,
            "val_55": 3,
            "val_60": 4,
            "val_65": 5,
            "val_70": 6,
        },
        "balanced": {
            "bal_equal": 1,
            "bal_alpha": 2,
            "bal_beta": 3,
            "bal_gamma": 4,
        },
    }

    return order_maps.get(scenario_group, {}).get(scenario_name, 999)


# =========================================================
# BUILD DATA UNTUK PLOT PER SCENARIO NAME
# =========================================================
def build_scenario_detail(df):
    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "overall_score",
    ]

    scenario_detail = (
        df
        .groupby(["scenario_group", "scenario_name"])[metric_cols]
        .mean()
        .reset_index()
    )

    scenario_detail["plot_order"] = scenario_detail.apply(
        lambda row: get_plot_order(
            row["scenario_group"],
            row["scenario_name"]
        ),
        axis=1
    )

    scenario_detail = scenario_detail.sort_values(
        by=["scenario_group", "plot_order"]
    ).reset_index(drop=True)

    return scenario_detail


# =========================================================
# PLOT 1: CORE METRICS PER SCENARIO GROUP
# Precision, Recall, F1-score
# =========================================================
def plot_core_metrics_by_scenario(scenario_detail):
    scenario_groups = scenario_detail["scenario_group"].unique()

    for group in scenario_groups:
        df_group = scenario_detail[
            scenario_detail["scenario_group"] == group
        ].copy()

        df_group = df_group.sort_values("plot_order")

        x = df_group["scenario_name"]

        plt.figure(figsize=(10, 6))
        plt.plot(x, df_group["Precision"], marker="o", label="Precision")
        plt.plot(x, df_group["Recall"], marker="o", label="Recall")
        plt.plot(x, df_group["F1_score"], marker="o", label="F1-score")

        plt.title(f"{group} - Precision, Recall, dan F1-score")
        plt.xlabel("Scenario Name")
        plt.ylabel("Score")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()

        output_path = os.path.join(
            PLOT_DIR,
            f"{group}_precision_recall_f1.png"
        )

        plt.savefig(output_path, dpi=300)
        plt.close()

        print(f"Plot tersimpan: {output_path}")


# =========================================================
# PLOT 2: SUPPORT METRICS PER SCENARIO GROUP
# NDCG, Fairness, Overall score
# =========================================================
def plot_support_metrics_by_scenario(scenario_detail):
    scenario_groups = scenario_detail["scenario_group"].unique()

    for group in scenario_groups:
        df_group = scenario_detail[
            scenario_detail["scenario_group"] == group
        ].copy()

        df_group = df_group.sort_values("plot_order")

        x = df_group["scenario_name"]

        plt.figure(figsize=(10, 6))
        plt.plot(x, df_group["NDCG"], marker="o", label="NDCG")
        plt.plot(x, df_group["Fairness"], marker="o", label="Fairness")
        plt.plot(x, df_group["overall_score"], marker="o", label="Overall score")

        plt.title(f"{group} - NDCG, Fairness, dan Overall Score")
        plt.xlabel("Scenario Name")
        plt.ylabel("Score")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()

        output_path = os.path.join(
            PLOT_DIR,
            f"{group}_ndcg_fairness_overall.png"
        )

        plt.savefig(output_path, dpi=300)
        plt.close()

        print(f"Plot tersimpan: {output_path}")


# =========================================================
# PLOT 3: RATA-RATA METRIK PER SCENARIO GROUP
# Untuk melihat scenario group mana yang paling stabil
# =========================================================
def plot_average_metrics_by_group(df):
    metric_cols = [
        "Precision",
        "Recall",
        "F1_score",
        "NDCG",
        "Fairness",
        "overall_score",
    ]

    group_summary = (
        df
        .groupby("scenario_group")[metric_cols]
        .mean()
        .reset_index()
    )

    group_summary.to_csv(
        "plot_scenario_group_average_summary.csv",
        index=False,
        encoding="utf-8-sig"
    )

    x = group_summary["scenario_group"]

    plt.figure(figsize=(12, 6))

    for metric in metric_cols:
        plt.plot(x, group_summary[metric], marker="o", label=metric)

    plt.title("Rata-rata Metrik Berdasarkan Scenario Group")
    plt.xlabel("Scenario Group")
    plt.ylabel("Average Score")
    plt.xticks(rotation=30)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = os.path.join(
        PLOT_DIR,
        "scenario_group_average_metrics.png"
    )

    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Plot tersimpan: {output_path}")
    print("Summary tersimpan: plot_scenario_group_average_summary.csv")


# =========================================================
# PLOT 4: F1-SCORE PER K UNTUK SETIAP SCENARIO GROUP
# Untuk melihat pengaruh K terhadap F1-score
# =========================================================
def plot_f1_by_k_and_scenario(df):
    f1_by_k = (
        df
        .groupby(["scenario_group", "K"])["F1_score"]
        .mean()
        .reset_index()
    )

    plt.figure(figsize=(10, 6))

    for group in f1_by_k["scenario_group"].unique():
        df_group = f1_by_k[f1_by_k["scenario_group"] == group]
        plt.plot(
            df_group["K"],
            df_group["F1_score"],
            marker="o",
            label=group
        )

    plt.title("Perbandingan F1-score terhadap Nilai K")
    plt.xlabel("K")
    plt.ylabel("Average F1-score")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = os.path.join(
        PLOT_DIR,
        "f1_score_by_k_and_scenario_group.png"
    )

    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Plot tersimpan: {output_path}")


# =========================================================
# PLOT 5: PRECISION DAN RECALL PER K
# Untuk melihat trade-off Precision dan Recall
# =========================================================
def plot_precision_recall_by_k(df):
    pr_by_k = (
        df
        .groupby("K")[["Precision", "Recall", "F1_score"]]
        .mean()
        .reset_index()
    )

    plt.figure(figsize=(10, 6))
    plt.plot(pr_by_k["K"], pr_by_k["Precision"], marker="o", label="Precision")
    plt.plot(pr_by_k["K"], pr_by_k["Recall"], marker="o", label="Recall")
    plt.plot(pr_by_k["K"], pr_by_k["F1_score"], marker="o", label="F1-score")

    plt.title("Precision, Recall, dan F1-score Berdasarkan Nilai K")
    plt.xlabel("K")
    plt.ylabel("Average Score")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()

    output_path = os.path.join(
        PLOT_DIR,
        "precision_recall_f1_by_k.png"
    )

    plt.savefig(output_path, dpi=300)
    plt.close()

    print(f"Plot tersimpan: {output_path}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    df = load_summary(SUMMARY_FILE)

    scenario_detail = build_scenario_detail(df)

    scenario_detail.to_csv(
        "plot_scenario_detail_summary.csv",
        index=False,
        encoding="utf-8-sig"
    )

    plot_core_metrics_by_scenario(scenario_detail)
    plot_support_metrics_by_scenario(scenario_detail)
    plot_average_metrics_by_group(df)
    plot_f1_by_k_and_scenario(df)
    plot_precision_recall_by_k(df)

    print("\nSelesai membuat plot.")
    print(f"Semua plot tersimpan di folder: {PLOT_DIR}")