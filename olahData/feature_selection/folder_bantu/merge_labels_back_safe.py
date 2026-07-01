import pandas as pd
import csv
from pathlib import Path

# =========================
# PATH FILE
# =========================
unique_path = Path(r"D:\Kuliah\DATA_TA\olahData\feature_selection\folder_bantu\manual_labeling_unique_by_query_product.csv")
full_path = Path(r"D:\Kuliah\DATA_TA\olahData\feature_selection\manual_labeling_multi_hybrid.csv")
output_path = Path(r"D:\Kuliah\DATA_TA\olahData\feature_selection\manual_labeling_multi_hybrid_final_labeled.csv")

# =========================
# LOAD FILE
# =========================
# File unique biasanya tersimpan dengan separator ; setelah diedit di Excel regional Indonesia.
df_unique = pd.read_csv(
    unique_path,
    sep=";",
    dtype=str,
    encoding="utf-8-sig",
    engine="python",
    quotechar='"',
    on_bad_lines="warn"
)

# File full original dari script Python biasanya separator koma.
df_full = pd.read_csv(
    full_path,
    sep=",",
    dtype=str,
    encoding="utf-8-sig",
    engine="python",
    quotechar='"',
    on_bad_lines="warn"
)

df_unique.columns = df_unique.columns.str.strip()
df_full.columns = df_full.columns.str.strip()

# =========================
# CEK KOLOM WAJIB
# =========================
required_unique = ["query", "name", "url", "manual_label", "label_note"]
required_full = ["query", "name", "url"]

missing_unique = [c for c in required_unique if c not in df_unique.columns]
missing_full = [c for c in required_full if c not in df_full.columns]

if missing_unique:
    raise ValueError(f"Kolom wajib tidak ada di file unique: {missing_unique}")

if missing_full:
    raise ValueError(f"Kolom wajib tidak ada di file full: {missing_full}")

# =========================
# NORMALIZED KEY
# =========================
# Pakai query + name + url, bukan id, karena id bisa berubah format saat file dibuka/simpan di Excel.
# Contoh: 102388000000 bisa berubah menjadi 1,02388E+11.
def make_key_cols(df):
    for col in ["query", "name", "url"]:
        df[f"__{col}_key"] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
        )
    return df

df_unique = make_key_cols(df_unique)
df_full = make_key_cols(df_full)

merge_keys = ["__query_key", "__name_key", "__url_key"]

# =========================
# AMBIL LABEL
# =========================
df_label = df_unique[
    merge_keys + ["manual_label", "label_note"]
].copy()

# Jika ada duplikat tidak sengaja di file unique, ambil yang pertama.
df_label = df_label.drop_duplicates(subset=merge_keys, keep="first")

# =========================
# MERGE LABEL KE FILE FULL
# =========================
df_full = df_full.drop(
    columns=[c for c in ["manual_label", "label_note"] if c in df_full.columns],
    errors="ignore"
)

df_final = df_full.merge(
    df_label,
    on=merge_keys,
    how="left",
    validate="many_to_one"
)

# Hapus kolom bantuan
df_final = df_final.drop(columns=merge_keys, errors="ignore")

# =========================
# SAVE
# =========================
output_path.parent.mkdir(parents=True, exist_ok=True)

df_final.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig",
    quoting=csv.QUOTE_ALL
)

# =========================
# SUMMARY
# =========================
print("Selesai merge label.")
print("Output:", output_path)
print("Jumlah baris file full:", len(df_full))
print("Jumlah baris final:", len(df_final))
print("Jumlah label terisi:", df_final["manual_label"].notna().sum())
print("Jumlah label kosong:", df_final["manual_label"].isna().sum())
print("\nDistribusi label:")
print(df_final["manual_label"].value_counts(dropna=False).sort_index())
