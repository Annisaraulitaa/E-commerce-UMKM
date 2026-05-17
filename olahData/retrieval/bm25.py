# =========================================================
# BM25 BASELINE & CANDIDATE RETRIEVAL
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

# TOP_K digunakan untuk hasil akhir baseline dan evaluasi metrik @K
TOP_K = 40
TOP_N_CANDIDATES = 2000   # 1-3% dari total dataset, sesuai proposal

TEXT_COLS = [
    "name_clean", 
    "category_clean", 
    "city_clean"
]

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
    return pd.Series([""] * len(df), index=df.index)


def basic_tokens(text: str):
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.split() if text else []


def tokenize_with_ngrams(text: str, max_n: int = 3):
    """Tokenisasi Unigram + Bigram + Trigram"""
    toks = basic_tokens(text)
    all_tokens = toks.copy()

    for n in range(2, max_n + 1):
        all_tokens.extend(
            "_".join(toks[i:i + n])
            for i in range(len(toks) - n + 1)
        )

    return all_tokens


def build_document(df: pd.DataFrame) -> pd.Series:
    """Membentuk dokumen teks untuk BM25."""
    name_clean = safe_get_col(df, "name_clean")
    category_clean = safe_get_col(df, "category_clean")
    city_clean = safe_get_col(df, "city_clean")

    doc = name_clean + " " + name_clean + " " + category_clean + " " + city_clean
    return doc.str.strip()


def normalize_minmax(series: pd.Series) -> pd.Series:
    """Normalisasi min-max ke rentang 0-1."""
    series = series.fillna(0).astype(float)
    min_val = series.min()
    max_val = series.max()

    if max_val == min_val:
        return pd.Series([0.0] * len(series), index=series.index)

    return (series - min_val) / (max_val - min_val)


def flexible_term_filter(df_out, q_unigrams):
    """Filter opsional agar hasil tidak terlalu luas."""
    n_terms = len(q_unigrams)

    if n_terms == 0:
        return df_out

    if n_terms <= 2:
        min_match = n_terms
    elif n_terms == 3:
        min_match = 2
    else:
        min_match = max(3, n_terms - 1)

    mask = df_out["doc_tokens"].apply(
        lambda s: sum(t in s for t in q_unigrams) >= min_match
    )
    return df_out[mask]


def select_output_columns(df_out: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in OUT_COLS if c in df_out.columns]
    cols += ["bm25_score", "bm25_norm"]
    return df_out[cols].reset_index(drop=True)


# =========================================================
# BUILD CORPUS & BM25 MODEL
# =========================================================
print("Membangun indeks BM25...")

df["doc"] = build_document(df)
df["doc_tokens"] = df["doc"].apply(lambda x: set(basic_tokens(x)))

# tokenisasi corpus
corpus_tokens = df["doc"].apply(lambda x: tokenize_with_ngrams(x, max_n=3)).tolist()
bm25 = BM25Okapi(corpus_tokens, k1=1.5, b=0.75)


# =========================================================
# CORE BM25 SCORING
# =========================================================
def bm25_all_scores(query: str) -> pd.DataFrame:
    """
    Menghitung skor BM25 untuk seluruh dokumen.
    Dipakai sebagai dasar untuk search dan candidate retrieval.
    """
    query = str(query).strip()
    if not query:
        return pd.DataFrame()

    q_tokens = tokenize_with_ngrams(query, max_n=3)
    scores = bm25.get_scores(q_tokens)

    df_out = df.copy()
    df_out["bm25_score"] = scores
    df_out = df_out[df_out["bm25_score"] > 0] # hanya pertahankan dokumen dengan skor positif

    if df_out.empty:
        return pd.DataFrame()
    
    df_out = df_out.sort_values("bm25_score", ascending=False).reset_index(drop=True)
    df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

    return df_out


# =========================================================
# BM25 SEARCH (BASELINE)
# =========================================================
def bm25_search(query: str, topk: int = TOP_K, use_term_filter: bool = False) -> pd.DataFrame:
    """
    BM25 baseline pure relevance.
    """
    query = str(query).strip()
    if not query:
        return pd.DataFrame(columns=OUT_COLS + ["bm25_score", "bm25_norm"])
    
    df_out = bm25_all_scores(query)
    
    if df_out.empty:
        return pd.DataFrame(columns=OUT_COLS + ["bm25_score", "bm25_norm"])
        
    # filter token opsional untuk query panjang
    if use_term_filter:
        df_out = flexible_term_filter(df_out, basic_tokens(query))
        df_out = df_out.reset_index(drop=True)

    df_out = df_out.head(topk)

    return df_out.reset_index(drop=True)


# =========================================================
# BM25 CANDIDATES FOR HYBRID
# =========================================================
def bm25_candidates(query: str, top_n: int = TOP_N_CANDIDATES) -> pd.DataFrame:
    """
    Candidate retrieval untuk hybrid.
    Mengambil top-n kandidat BM25 teratas sebelum UMKM filtering dan re-ranking.
    """
    query = str(query).strip()
    if not query:
        return pd.DataFrame()

    df_out = bm25_all_scores(query)

    if df_out.empty:
        return pd.DataFrame()

    return df_out.head(top_n).reset_index(drop=True)


# =========================================================
# MAIN EXECUTION
# =========================================================
if __name__ == "__main__":
    print("BM25 baseline siap digunakan.")
    q = input("Masukkan query: ").strip()

    # 1) Output final baseline
    baseline_result = bm25_search(
        q,
        topk=TOP_K,
        use_term_filter=False
    )

    print(f"\n=== HASIL BM25 BASELINE TOP-{TOP_K} ===")
    print(baseline_result.to_string(index=False))

    baseline_result.to_csv("bm25_baseline_topk.csv", index=False, encoding="utf-8-sig")
    print("\nDisimpan: bm25_baseline_topk.csv")

    # 2) Candidate pool BM25 untuk tahap hybrid
    candidate_pool = bm25_candidates(
        q, 
        top_n=TOP_N_CANDIDATES
    )

    print(f"\nJumlah candidate pool untuk hybrid: {len(candidate_pool)}")
    candidate_pool.to_csv("bm25_candidates_topn.csv", index=False, encoding="utf-8-sig")
    print("Disimpan: bm25_candidates_topn.csv")