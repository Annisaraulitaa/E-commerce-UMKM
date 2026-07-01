import pandas as pd
import csv
from pathlib import Path

# =========================
# PATH FILE
# =========================
input_path = r"D:\Kuliah\DATA_TA\olahData\feature_selection\manual_labeling_multi_hybrid_final_labeled.csv"

output_path = r"D:\Kuliah\DATA_TA\olahData\feature_selection\manual_labeling_multi_hybrid_final_labeled_10_queries.csv"

# =========================
# QUERY YANG DIHAPUS
# =========================
remove_queries = [
    "sepatu sneakers",
    "tas selempang",
    "kain batik tulis motif parang solo",
    "dompet kulit handmade",
    "oleh-oleh makanan khas sulawesi",
]

# =========================
# LOAD CSV
# =========================
df = pd.read_csv(
    input_path,
    engine="python",
    sep=",",
    quotechar='"',
    dtype=str,
    encoding="utf-8-sig",
    on_bad_lines="skip"
)

# Bersihkan nama kolom dari spasi tersembunyi
df.columns = df.columns.str.strip()

# Pastikan kolom query ada
if "query" not in df.columns:
    raise ValueError("Kolom 'query' tidak ditemukan di file.")

# =========================
# FILTER DATA
# =========================
df_filtered = df[~df["query"].isin(remove_queries)].copy()

# =========================
# SAVE FILE BARU
# =========================
Path(output_path).parent.mkdir(parents=True, exist_ok=True)

df_filtered.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL
)

# =========================
# INFO HASIL
# =========================
print("Selesai filter query.")
print("File output:", output_path)
print("Jumlah baris awal:", len(df))
print("Jumlah baris setelah filter:", len(df_filtered))
print("Jumlah query awal:", df["query"].nunique())
print("Jumlah query setelah filter:", df_filtered["query"].nunique())

print("\nQuery yang tersisa:")
print(df_filtered["query"].value_counts().sort_index())