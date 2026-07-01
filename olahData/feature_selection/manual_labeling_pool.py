# =========================================================
# BM25 CANDIDATE RETRIEVAL — MULTI QUERY FOR HYBRID
# Tanpa TOP-K baseline, hanya menyimpan TOP-N kandidat per query
# =========================================================
"""
Output:
output_bm25_candidates/
└── manual_labeling_multi_hybrid.csv

File output ini berisi:
- query
- rank_bm25
- bm25_score
- bm25_norm
- fitur popularity: countSold, countReview, totalRating
- fitur value: ratingAverage, discountPercentage
- fitur UMKM: umkm_label, shop_umkm_label

Cara menjalankan:
python manual_labeling_pool.py --input "D:/Kuliah/DATA_TA/olahData/retrieval/nondup_labeled_dataset(new).csv"
"""

import argparse
import os
import re
from pathlib import Path

import pandas as pd
from rank_bm25 import BM25Okapi


# =========================================================
# KONFIGURASI
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DEFAULT_CSV_PATH = os.path.join(BASE_DIR, "D:/Kuliah/DATA_TA/olahData/retrieval/nondup_labeled_dataset(new).csv")
ENCODING = "utf-8"

TOP_N_CANDIDATES = 2000

DEFAULT_QUERIES = [
    # ── UMKM-Specific (5 query) ──────────────────────────────
    # Query yang secara eksplisit mengandung nuansa lokal/UMKM
    "kopi khas daerah",            # makanan, panjang sedang
    "kain tenun tradisional",      # kerajinan tekstil
    "dompet kulit handmade",       ## aksesoris, eksplisit handmade
    "keripik homemade",            # makanan rumahan
    "kerajinan anyaman",           # kerajinan, pendek

    # ── Netral (5 query) ─────────────────────────────────────
    # Query yang bisa mengarah ke UMKM atau brand besar
    "baju batik wanita",           # fashion, kompetitif
    "tas selempang",               ## aksesoris, netral
    "sepatu pria",                 # fashion, pendek & kompetitif
    "camilan snack",               # makanan ringan, umum
    "hiasan dinding ruang tamu",   # rumah tangga, panjang/spesifik

    # ── Brand-Prone (3 query) ────────────────────────────────
    # Query yang biasanya didominasi produk besar/non-UMKM
    # → untuk menguji apakah sistem tetap bisa munculkan UMKM
    "kemeja casual",               # fashion massal
    "sepatu sneakers",             ## brand-dominated
    "tas ransel laptop",           # elektronik/aksesoris massal

    # ── Panjang & Spesifik (2 query) ─────────────────────────
    # Menguji kemampuan BM25 pada query detail
    "kain batik tulis motif parang solo",   ## sangat spesifik
    "oleh-oleh makanan khas sulawesi",      ## spesifik lokasi
]

OUT_COLS = [
    # identitas produk
    "id",
    "name",
    "url",

    # kategori dan teks
    "category_breadcrumb",
    "category_clean",
    "name_clean",
    "city_clean",

    # harga dan value
    "price_number",
    "discountPercentage",
    "ratingAverage",

    # toko
    "shop_id",
    "shop_name",
    "shop_city",
    "shop_tier",

    # popularity
    "countSold",
    "countReview",
    "totalRating",

    # UMKM
    "umkm_label",
    "shop_umkm_label",
]


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
    toks = basic_tokens(text)
    all_tokens = toks.copy()

    for n in range(2, max_n + 1):
        all_tokens.extend(
            "_".join(toks[i:i + n])
            for i in range(len(toks) - n + 1)
        )

    return all_tokens


def build_document(df: pd.DataFrame) -> pd.Series:
    name_clean = safe_get_col(df, "name_clean")
    category_clean = safe_get_col(df, "category_clean")
    city_clean = safe_get_col(df, "city_clean")

    # name_clean dibuat dua kali agar nama produk lebih dominan.
    doc = name_clean + " " + category_clean + " " + city_clean
    return doc.str.strip()


def normalize_minmax(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce").fillna(0).astype(float)
    min_val = series.min()
    max_val = series.max()

    if max_val == min_val:
        return pd.Series([0.0] * len(series), index=series.index)

    return (series - min_val) / (max_val - min_val)


def select_output_columns(df_out: pd.DataFrame) -> pd.DataFrame:
    fixed_cols = ["query", "rank_bm25"]
    feature_cols = [c for c in OUT_COLS if c in df_out.columns]
    score_cols = ["bm25_score", "bm25_norm"]

    cols = fixed_cols + feature_cols + score_cols
    cols = [c for c in cols if c in df_out.columns]

    return df_out[cols].reset_index(drop=True)


def get_topk(df, score_col, k=70):
    return (
        df.sort_values(score_col, ascending=False)
            .head(k)
            .copy()
    )


def compute_hybrid_scores(df: pd.DataFrame):

    # --- normalization ---
    df["bm25_norm"] = normalize_minmax(df["bm25_score"])

    df["popularity_norm"] = normalize_minmax(
        df["countSold"].astype(float) +
        df["countReview"].astype(float) +
        df["totalRating"].astype(float)
    )

    df["value_norm"] = normalize_minmax(
        df["ratingAverage"].astype(float) +
        df["discountPercentage"].astype(float)
    )

    # =========================
    # 1. BALANCE
    # =========================
    df["score_balance"] = (
        0.34 * df["bm25_norm"] +
        0.33 * df["popularity_norm"] +
        0.33 * df["value_norm"]
    )

    # =========================
    # 2. RELEVANCE DOMINANT
    # =========================
    df["score_relevance"] = (
        0.50 * df["bm25_norm"] +
        0.25 * df["popularity_norm"] +
        0.25 * df["value_norm"]
    )

    # =========================
    # 3. POPULARITY DOMINANT
    # =========================
    df["score_popularity"] = (
        0.50 * df["popularity_norm"] +
        0.25 * df["bm25_norm"] +
        0.25 * df["value_norm"]
    )

    # =========================
    # 4. VALUE DOMINANT
    # =========================
    df["score_value"] = (
        0.50 * df["value_norm"] +
        0.25 * df["bm25_norm"] +
        0.25 * df["popularity_norm"]
    )

    return df


# =========================================================
# BM25 ENGINE
# =========================================================
class BM25CandidateEngine:
    def __init__(self, csv_path: str, encoding: str = ENCODING):
        print(f"Membaca dataset: {csv_path}")
        self.df = pd.read_csv(csv_path, encoding=encoding, low_memory=False)

        print(f"Dataset: {self.df.shape[0]:,} baris × {self.df.shape[1]} kolom")
        print("Membangun indeks BM25...")

        self.df["doc"] = build_document(self.df)

        corpus_tokens = (
            self.df["doc"]
            .apply(lambda x: tokenize_with_ngrams(x, max_n=3))
            .tolist()
        )

        self.bm25 = BM25Okapi(corpus_tokens, k1=1.5, b=0.75)
        print("BM25 siap digunakan.")


    def get_candidates(self, query: str, top_n: int = TOP_N_CANDIDATES) -> pd.DataFrame:
        query = str(query).strip()
        if not query:
            return pd.DataFrame()

        q_tokens = tokenize_with_ngrams(query, max_n=3)
        scores = self.bm25.get_scores(q_tokens)

        df_out = self.df.copy()
        df_out["bm25_score"] = scores

        # Ambil dokumen yang punya skor BM25 positif saja.
        df_out = df_out[df_out["bm25_score"] > 0]

        if df_out.empty:
            return pd.DataFrame()

        df_out = df_out.sort_values("bm25_score", ascending=False).reset_index(drop=True)
        df_out = df_out.head(top_n).copy()

        # Normalisasi dilakukan per query agar BM25 berada pada rentang 0-1 untuk query tersebut.
        df_out["bm25_norm"] = normalize_minmax(df_out["bm25_score"])

        df_out["query"] = query
        df_out["rank_base"] = range(1, len(df_out) + 1)

        return select_output_columns(df_out)
    

    def run_many_queries(self, queries: list[str], top_n: int = TOP_N_CANDIDATES):

        output_path = Path(__file__).resolve().parent

        all_results = []

        for i, query in enumerate(queries):

            print(f"[{i+1}/{len(queries)}] {query}")

            df_q = self.get_candidates(query, top_n=top_n)

            if df_q.empty:
                continue

            # =========================
            # HYBRID SCORES
            # =========================
            df_q = compute_hybrid_scores(df_q)

            # =========================
            # 4 MODEL OUTPUTS
            # =========================

            models = {
                "bm25": "bm25_score",
                "balance": "score_balance",
                "relevance": "score_relevance",
                "popularity": "score_popularity",
                "value": "score_value",
            }

            for model_name, score_col in models.items():

                df_model = get_topk(df_q, score_col, k=70)

                df_model["model"] = model_name
                df_model["query"] = query
                df_model["rank"] = range(1, len(df_model) + 1)

                df_model["manual_label"] = ""
                df_model["label_note"] = ""

                all_results.append(df_model)

        final_df = pd.concat(all_results, ignore_index=True)

        output_file = output_path / "manual_labeling_multi_hybrid.csv"

        final_df.to_csv(output_file, index=False, encoding="utf-8-sig")

        print("\nDONE")
        print("Saved:", output_file)

        return final_df


# =========================================================
# MAIN
# =========================================================
def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        default=DEFAULT_CSV_PATH,
        help="Path dataset produk/preprocessed.",
    )

    parser.add_argument(
        "--output",
        default="output_bm25_candidates",
        help="Folder output.",
    )

    parser.add_argument(
        "--topn",
        type=int,
        default=TOP_N_CANDIDATES,
        help="Jumlah kandidat BM25 per query untuk hybrid. Ini bukan K evaluasi.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    print("\nDaftar query yang akan dijalankan:")
    for i, q in enumerate(DEFAULT_QUERIES, start=1):
        print(f"{i}. {q}")

    engine = BM25CandidateEngine(csv_path=args.input, encoding=ENCODING)

    engine.run_many_queries(
        queries=DEFAULT_QUERIES,
        top_n=args.topn
    )
