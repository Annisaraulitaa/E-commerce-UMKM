# =========================================================
# IMPORT LIBRARY
# =========================================================
import os
import re
import pandas as pd
from rank_bm25 import BM25Okapi


# =========================================================
# KONFIGURASI
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# path dataset utama yang akan dipakai
CSV_PATH = os.path.join(BASE_DIR, "nondup_labeled_dataset.csv")

ENCODING = "utf-8"
TOPK = 20

# kolom teks yang dipakai untuk membangun dokumen BM25
TEXT_COLS = [
    "name_clean", 
    "category_clean", 
    "city_clean"
]

# kolom yang ingin ditampilkan pada hasil pencarian
OUT_COLS = [
    "id", "name", "url",
    "category_breadcrumb",
    "price_number", "discountPercentage",
    "ratingAverage", "shop_id", "shop_name",
    "shop_city", "shop_tier", "countSold",
    "name_clean","has_promo", "umkm_label",
]


# =========================================================
# LOAD DATA
# =========================================================
df = pd.read_csv(CSV_PATH, encoding=ENCODING)


# =========================================================
# UTIL FUNCTIONS
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


# menambah n-gram ke token biasa
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


# menggabungkan beberapa kolom teks menjadi satu dokumen yang nanti diindeks BM25
def build_document(df: pd.DataFrame) -> pd.Series:
    parts = [safe_get_col(df, c) for c in TEXT_COLS]
    doc = parts[0]
    for p in parts[1:]:
        doc = doc + " " + p
    return doc


# normalisasi skor ke rentang 0-1 untuk memudahkan penggabungan dengan faktor lain di hybrid ranking
def normalize_minmax(series):
    series = series.fillna(0).astype(float)
    if series.max() == series.min():
        return pd.Series([0] * len(series), index=series.index)
    return (series - series.min()) / (series.max() - series.min())


# =========================================================
# BUILD CORPUS & BM25 MODEL
# =========================================================
print("Membangun indeks BM25...")

# membuat kolom dokumen gabungan
df["doc"] = build_document(df)

# setiap dokumen diubah menjadi token unigram+bigram+trigram
corpus_tokens = df["doc"].apply(
    lambda x: tokenize_with_ngrams(x, max_n=3)
).tolist()

# membangun model BM25 dengan parameter k1=1.5 dan b=0.75 (nilai standar yang cukup umum dipakai)
bm25 = BM25Okapi(corpus_tokens, k1=1.5, b=0.75)


# =========================================================
# SEARCH FUNCTIONS
# =========================================================
def bm25_search(query: str, topk: int = 20, require_all_terms: bool = False):
    """
    Fungsi pencarian BM25 dengan opsi AND-filter untuk query panjang
    """
    q_tokens = tokenize_with_ngrams(query, max_n=3)
    q_unigrams = basic_tokens(query)

    # hitung skor BM25 semua dokumen terhadap query
    scores = bm25.get_scores(q_tokens)

    # simpan skor asli dan skor normalisasi
    df_out = df.copy()
    df_out["bm25_score"] = scores
    df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

    # AND Filter (opsional untuk query panjang)
    if require_all_terms and q_unigrams:
        doc_tokens = df_out["doc"].apply(lambda x: set(basic_tokens(x)))
        mask = doc_tokens.apply(lambda s: all(t in s for t in q_unigrams))
        df_out = df_out[mask]

    # urutkan dari skor BM25 tertinggi, lalu ambil top-k
    df_out = df_out.sort_values("bm25_score", ascending=False).head(topk)

    cols = [c for c in OUT_COLS if c in df_out.columns]
    cols = cols + ["bm25_score", "bm25_norm"]

    return df_out[cols].reset_index(drop=True)


# fungsi ini dipakai bukan untuk hasil akhir ke user, tetapi untuk mengambil candidate pool awal. Hasil inilah yang nantinya masuk ke hybrid reranking
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
# MAIN EXECUTION
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

    result.to_csv("bm25_results8.csv", index=False, encoding="utf-8-sig")
    print("\nDisimpan: bm25_results8.csv")