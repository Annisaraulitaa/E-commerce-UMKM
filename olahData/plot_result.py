import pandas as pd
import matplotlib.pyplot as plt

# Load hasil ringkasan
df_summary = pd.read_csv("summary_multi_lambda.csv")

print(df_summary)

# =========================================================
# 1. LAMBDA VS PRECISION
# =========================================================
plt.figure(figsize=(8, 6))
plt.plot(df_summary["lambda"], df_summary["Precision@20"], marker="o")
plt.xlabel("Lambda UMKM")
plt.ylabel("Precision@20")
plt.title("Lambda vs Precision@20")
plt.grid(True)
plt.tight_layout()
plt.show()


# =========================================================
# 2. LAMBDA VS NDCG
# =========================================================
plt.figure(figsize=(8, 6))
plt.plot(df_summary["lambda"], df_summary["NDCG@20"], marker="o")
plt.xlabel("Lambda UMKM")
plt.ylabel("NDCG@20")
plt.title("Lambda vs NDCG@20")
plt.grid(True)
plt.tight_layout()
plt.show()


# =========================================================
# 3. LAMBDA VS FAIRNESS
# =========================================================
plt.figure(figsize=(8, 6))
plt.plot(df_summary["lambda"], df_summary["Fairness@20"], marker="o")
plt.xlabel("Lambda UMKM")
plt.ylabel("Fairness@20")
plt.title("Lambda vs Fairness@20")
plt.grid(True)
plt.tight_layout()
plt.show()


# =========================================================
# 4. FAIRNESS VS NDCG TRADEOFF
# =========================================================
plt.figure(figsize=(8, 6))
plt.plot(df_summary["Fairness@20"], df_summary["NDCG@20"], marker="o")

for _, row in df_summary.iterrows():
    plt.annotate(
        f"λ={row['lambda']}",
        (row["Fairness@20"], row["NDCG@20"]),
        xytext=(5, 5),
        textcoords="offset points"
    )

plt.xlabel("Fairness@20")
plt.ylabel("NDCG@20")
plt.title("Fairness vs Relevance Tradeoff Curve")
plt.grid(True)
plt.tight_layout()
plt.show()