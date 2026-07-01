import csv
from pathlib import Path

# File input utama dari output multi-model
input_path = r"D:\Kuliah\DATA_TA\olahData\feature_selection\manual_labeling_multi_hybrid.csv"

# File output unik untuk dilabel manual
output_unique_path = r"D:\Kuliah\DATA_TA\olahData\feature_selection\folder_bantu\manual_labeling_unique_by_query_product.csv"

# Pakai query + id + name agar produk yang sama pada query yang sama cukup dilabel 1 kali.
# Jangan pakai kolom model, karena produk yang sama bisa muncul di BM25, balance, relevance, popularity, dan value.
DEDUP_KEYS = ["query", "id", "name"]

LABELING_COLS = [
    "query",
    "id",
    "name",
    "url",
    "category_breadcrumb",
    "manual_label",
    "label_note",
]

seen = set()
unique_rows = []
total_rows = 0

with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames or []

    missing_keys = [col for col in DEDUP_KEYS if col not in headers]
    if missing_keys:
        raise ValueError(f"Kolom dedup tidak ditemukan: {missing_keys}. Kolom tersedia: {headers}")

    existing_cols = [col for col in LABELING_COLS if col in headers]

    for row in reader:
        total_rows += 1

        key = tuple((row.get(col) or "").strip() for col in DEDUP_KEYS)

        if key in seen:
            continue

        seen.add(key)

        out_row = {col: row.get(col, "") for col in existing_cols}
        out_row["manual_label"] = ""
        out_row["label_note"] = ""

        unique_rows.append(out_row)

Path(output_unique_path).parent.mkdir(parents=True, exist_ok=True)

with open(output_unique_path, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=existing_cols, quoting=csv.QUOTE_ALL)
    writer.writeheader()
    writer.writerows(unique_rows)

print("Selesai membuat file unik untuk labeling.")
print("File output:", output_unique_path)
print("Jumlah baris awal:", total_rows)
print("Jumlah baris unik:", len(unique_rows))
print("Duplikat yang dihilangkan:", total_rows - len(unique_rows))
