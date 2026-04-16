import pandas as pd
import matplotlib.pyplot as plt


# =========================================================
# 1. LOAD DATA
# =========================================================
df_eval = pd.read_csv("evaluation_results_final.csv")
df_summary = pd.read_csv("summary_final.csv")

print("=== evaluation_results_final.csv ===")
print(df_eval)

print("\n=== summary_final.csv ===")
print(df_summary)


# =========================================================
# 2. BAR CHART RINGKASAN SEMUA METRIK
# =========================================================
metrics = [
    "Precision@20",
    "Recall@20",
    "NDCG@20",
    "Fairness@20",
    "ExposureDisparity@20"
]

df_plot = df_summary.set_index("method")[metrics].T

plt.figure(figsize=(10, 6))
df_plot.plot(kind="bar")
plt.title("Perbandingan BM25 vs Hybrid_Final")
plt.xlabel("Metrik")
plt.ylabel("Nilai")
plt.xticks(rotation=0)
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()


# =========================================================
# 3. FAIRNESS VS EXPOSURE DISPARITY
# =========================================================
plt.figure(figsize=(8, 6))

for _, row in df_summary.iterrows():
    plt.scatter(
        row["Fairness@20"],
        row["ExposureDisparity@20"],
        s=100,
        label=row["method"]
    )
    plt.annotate(
        row["method"],
        (row["Fairness@20"], row["ExposureDisparity@20"]),
        xytext=(5, 5),
        textcoords="offset points"
    )

plt.title("Fairness vs Exposure Disparity")
plt.xlabel("Fairness@20")
plt.ylabel("ExposureDisparity@20")
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()


# =========================================================
# 4. PER QUERY: PRECISION DAN FAIRNESS
# =========================================================
pivot_precision = df_eval.pivot(index="query", columns="method", values="Precision@20")
pivot_fairness = df_eval.pivot(index="query", columns="method", values="Fairness@20")

plt.figure(figsize=(10, 6))
pivot_precision.plot(kind="bar")
plt.title("Precision@20 per Query")
plt.xlabel("Query")
plt.ylabel("Precision@20")
plt.xticks(rotation=30, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
pivot_fairness.plot(kind="bar")
plt.title("Fairness@20 per Query")
plt.xlabel("Query")
plt.ylabel("Fairness@20")
plt.xticks(rotation=30, ha="right")
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()