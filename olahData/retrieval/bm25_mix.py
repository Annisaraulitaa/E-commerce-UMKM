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

CSV_PATH = os.path.join(BASE_DIR, "nondup_labeled_dataset(new).csv")

ENCODING = "utf-8"
K = 40
N_CANDIDATES = 2000   # fixed top-n
N_CANDIDATES_MAX = 5000   # cap untuk adaptive top-n

TEXT_COLS = ["name_clean", "category_clean", "city_clean"]

OUT_COLS = [
    "id", "name", "url",
    "category_breadcrumb",
    "price_number", "discountPercentage",
    "ratingAverage", "shop_id", "shop_name",
    "shop_city", "shop_tier", "countSold",
    "name_clean", "has_promo", "umkm_label",
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
    return pd.Series([""] * len(df), index=df.index)


def basic_tokens(text: str):
    text = str(text).lower()
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
    """
    Membentuk dokumen teks untuk BM25.
    name_clean diberi bobot lebih besar dengan dimasukkan dua kali.
    """
    parts = [safe_get_col(df, c) for c in TEXT_COLS]

    doc = parts[0] + " " + parts[0]
    for p in parts[1:]:
        doc = doc + " " + p

    return doc.str.strip()


def normalize_minmax(series):
    series = series.fillna(0).astype(float)
    if series.max() == series.min():
        return pd.Series([0.0] * len(series), index=series.index)
    return (series - series.min()) / (series.max() - series.min())


def flexible_term_filter(df_out, q_unigrams):
    """
    Filter fleksibel berdasarkan jumlah token query.
    """
    n_terms = len(q_unigrams)

    if n_terms == 0:
        return df_out

    if n_terms <= 3:
        min_match = n_terms
    else:
        min_match = max(3, n_terms - 1)

    mask = df_out["doc_tokens"].apply(
        lambda s: sum(t in s for t in q_unigrams) >= min_match
    )

    return df_out[mask]


# =========================================================
# BUILD CORPUS & BM25 MODEL
# =========================================================
print("Membangun indeks BM25...")

df["doc"] = build_document(df)
df["doc_tokens"] = df["doc"].apply(lambda x: set(basic_tokens(x)))

corpus_tokens = df["doc"].apply(
    lambda x: tokenize_with_ngrams(x, max_n=3)
).tolist()

bm25 = BM25Okapi(corpus_tokens, k1=1.5, b=0.75)


# =========================================================
# CORE RETRIEVAL
# =========================================================
def bm25_all_scores(query: str):
    """
    Hitung skor BM25 untuk seluruh dokumen.
    Ini dipakai sebagai dasar baik untuk fixed maupun adaptive retrieval.
    """
    query = str(query).strip()
    if not query:
        return pd.DataFrame()

    q_tokens = tokenize_with_ngrams(query, max_n=3)
    scores = bm25.get_scores(q_tokens)

    df_out = df.copy()
    df_out["bm25_score"] = scores
    df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

    # hanya simpan dokumen yang benar-benar punya kecocokan
    df_out = df_out[df_out["bm25_score"] > 0]

    return df_out.sort_values("bm25_score", ascending=False).reset_index(drop=True)


# =========================================================
# BM25 SEARCH (BASELINE)
# =========================================================
def bm25_search(query: str, topk: int = K, use_term_filter: bool = False):
    """
    BM25 baseline pure relevance.
    """
    query = str(query).strip()
    if not query:
        return pd.DataFrame(columns=OUT_COLS + ["bm25_score", "bm25_norm"])

    q_unigrams = basic_tokens(query)
    df_out = bm25_all_scores(query)

    if df_out.empty:
        return pd.DataFrame(columns=OUT_COLS + ["bm25_score", "bm25_norm"])

    if use_term_filter and q_unigrams:
        df_out = flexible_term_filter(df_out, q_unigrams)

    df_out = df_out.head(topk)

    cols = [c for c in OUT_COLS if c in df_out.columns]
    cols += ["bm25_score", "bm25_norm"]

    return df_out[cols].reset_index(drop=True)


# =========================================================
# FIXED TOP-N CANDIDATES
# =========================================================
def bm25_candidates_fixed(query: str, top_n: int = N_CANDIDATES):
    """
    Candidate retrieval fixed:
    ambil top-n kandidat BM25 teratas.
    """
    df_out = bm25_all_scores(query)
    if df_out.empty:
        return pd.DataFrame()

    return df_out.head(top_n).reset_index(drop=True)


# =========================================================
# ADAPTIVE TOP-N CANDIDATES
# =========================================================
def bm25_candidates_adaptive(query: str, top_n_max: int = N_CANDIDATES_MAX):
    """
    Candidate retrieval adaptif:
    - ambil semua dokumen dengan bm25_score > 0
    - lalu batasi maksimum top_n_max
    """
    df_out = bm25_all_scores(query)
    if df_out.empty:
        return pd.DataFrame()

    actual_pool_size = len(df_out)

    if top_n_max is not None:
        df_out = df_out.head(top_n_max)

    df_out = df_out.reset_index(drop=True)
    df_out["adaptive_pool_size_before_cap"] = actual_pool_size

    return df_out


# =========================================================
# DEFAULT CANDIDATE FUNCTION FOR HYBRID
# =========================================================
def bm25_candidates(query: str, top_n: int = N_CANDIDATES, mode: str = "fixed", top_n_max: int = N_CANDIDATES_MAX):
    """
    Wrapper umum agar hybrid bisa memilih mode retrieval.
    mode:
    - 'fixed'
    - 'adaptive'
    """
    if mode == "adaptive":
        return bm25_candidates_adaptive(query, top_n_max=top_n_max)
    return bm25_candidates_fixed(query, top_n=top_n)


# =========================================================
# MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    print("BM25 siap digunakan.")
    q = input("Masukkan query: ").strip()

    # hasil baseline BM25
    result = bm25_search(
        q,
        topk=K,
        use_term_filter=True
    )

    print("\n=== HASIL BM25 SEARCH ===")
    print(result.to_string(index=False))

    result.to_csv("bm25_results.csv", index=False, encoding="utf-8-sig")
    print("\nDisimpan: bm25_results.csv")

    # contoh fixed candidates
    fixed_candidates = bm25_candidates_fixed(q, top_n=N_CANDIDATES)
    print(f"\nJumlah fixed candidates: {len(fixed_candidates)}")

    # contoh adaptive candidates
    adaptive_candidates = bm25_candidates_adaptive(q, top_n_max=N_CANDIDATES_MAX)
    if not adaptive_candidates.empty and "adaptive_pool_size_before_cap" in adaptive_candidates.columns:
        actual_size = adaptive_candidates["adaptive_pool_size_before_cap"].iloc[0]
    else:
        actual_size = 0

    print(f"Jumlah adaptive candidates setelah cap : {len(adaptive_candidates)}")
    print(f"Jumlah adaptive candidates sebelum cap: {actual_size}")

    fixed_candidates.to_csv("bm25_candidates_fixed.csv", index=False, encoding="utf-8-sig")
    adaptive_candidates.to_csv("bm25_candidates_adaptive.csv", index=False, encoding="utf-8-sig")

    print("\nDisimpan:")
    print("- bm25_candidates_fixed.csv")
    print("- bm25_candidates_adaptive.csv")