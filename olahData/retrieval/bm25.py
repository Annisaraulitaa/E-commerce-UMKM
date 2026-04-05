# =========================================================
# 1. IMPORT LIBRARY
# =========================================================
import re
import pickle
import pandas as pd
from rank_bm25 import BM25Okapi


# =========================================================
# 2. KONFIGURASI
# =========================================================
CSV_PATH = "nondup_labeled_dataset.csv"
ENCODING = "utf-8"
TOPK = 20

TEXT_COLS = [
    "name_clean", 
    "category_clean", 
    "city_clean"
]

OUT_COLS = [
    "id", "name", "url",
    "category_breadcrumb",
    "price_number",          
    "discountPercentage",
    "ratingAverage", "shop_id",
    "shop_name", "shop_city", "shop_tier",
    "countSold", "has_Promo", "umkm_label",
]


# =========================================================
# 3. LOAD DATA
# =========================================================
df = pd.read_csv(CSV_PATH, encoding=ENCODING)

# Rename kolom jika diperlukan
rename_map = {
    "shop.id": "shop_id",
    "shop.name": "shop_name",
    "shop.city": "shop_city",
    "shop.tier": "shop_tier",
    "category.breadcrumb": "category_breadcrumb",
    "price.number": "price_number",
    "price.discountPercentage": "discountPercentage",
}

for old, new in rename_map.items():
    if old in df.columns and new not in df.columns:
        df = df.rename(columns={old: new})

# Fallback jika kolom clean belum tersedia
if "name_clean" not in df.columns:
    df["name_clean"] = df["name"].fillna("").astype(str).str.lower()

if "category_clean" not in df.columns:
    df["category_clean"] = df["category_breadcrumb"].fillna("").astype(str).str.lower()

if "city_clean" not in df.columns:
    df["city_clean"] = df["shop_city"].fillna("").astype(str).str.lower()


# =========================================================
# 4. UTIL FUNCTIONS
# =========================================================
def safe_get_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].fillna("").astype(str)
    return pd.Series([""] * len(df))


def basic_tokens(text: str):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.split() if text else []


def tokenize_with_ngrams(text: str, max_n: int = 3):
    """
    Tokenisasi Unigram + Bigram + Trigram
    """
    toks = basic_tokens(text)
    all_tokens = toks.copy()

    for n in range(2, max_n + 1):
        ngrams = [
            "_".join(toks[i:i+n])
            for i in range(len(toks) - n + 1)
        ]
        all_tokens.extend(ngrams)

    return all_tokens


def build_document(df: pd.DataFrame) -> pd.Series:
    parts = [safe_get_col(df, c) for c in TEXT_COLS]
    doc = parts[0]
    for p in parts[1:]:
        doc = doc + " " + p
    return doc


def normalize_minmax(series):
    series = series.fillna(0).astype(float)
    if series.max() == series.min():
        return pd.Series([0] * len(series), index=series.index)
    return (series - series.min()) / (series.max() - series.min())


# =========================================================
# 5. BUILD CORPUS & BM25 MODEL
# =========================================================
print("Membangun indeks BM25...")

df["doc"] = build_document(df)
corpus_tokens = df["doc"].apply(
    lambda x: tokenize_with_ngrams(x, max_n=3)
).tolist()

bm25 = BM25Okapi(corpus_tokens, k1=1.5, b=0.75)


# =========================================================
# 6. SEARCH FUNCTIONS
# =========================================================
def bm25_search(query: str, topk: int = 20, require_all_terms: bool = False):
    """
    Fungsi pencarian BM25 dengan opsi AND-filter untuk query panjang
    """
    q_tokens = tokenize_with_ngrams(query, max_n=3)
    q_unigrams = basic_tokens(query)

    scores = bm25.get_scores(q_tokens)

    df_out = df.copy()
    df_out["bm25_score"] = scores
    df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

    # AND Filter (opsional untuk query panjang)
    if require_all_terms and q_unigrams:
        doc_tokens = df_out["doc"].apply(lambda x: set(basic_tokens(x)))
        mask = doc_tokens.apply(lambda s: all(t in s for t in q_unigrams))
        df_out = df_out[mask]

    df_out = df_out.sort_values("bm25_score", ascending=False).head(topk)

    cols = [c for c in OUT_COLS if c in df_out.columns]
    cols = cols + ["bm25_score", "bm25_norm"]

    return df_out[cols].reset_index(drop=True)


def bm25_candidates(query: str, top_n: int = 2000):
    """
    Candidate retrieval untuk Hybrid Ranking (Top-N ±2% dataset)
    """
    q_tokens = tokenize_with_ngrams(query, max_n=3)
    scores = bm25.get_scores(q_tokens)

    df_out = df.copy()
    df_out["bm25_score"] = scores
    df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

    return df_out.sort_values("bm25_score", ascending=False).head(top_n)


# =========================================================
# 7. MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    print("BM25 siap digunakan.")
    q = input("Masukkan query: ").strip()

    result = bm25_search(
        q,
        topk=TOPK,
        require_all_terms=len(q.split()) >= 3
    )

    print("\nHasil Pencarian:")
    print(result.to_string(index=False))

    result.to_csv("bm25_results6.csv", index=False, encoding="utf-8-sig")
    print("\nDisimpan: bm25_results6.csv")