# =========================================================
# CREATE POOLED MANUAL GROUND TRUTH TEMPLATE
# =========================================================
# Output: manual_pool_labeling_template.csv
# Isi kolom manual_relevance secara manual:
# 1 = relevan, 0 = tidak relevan
# =========================================================

import pandas as pd
from retrieval.bm25 import bm25_search
from reranking.hybrid_rerank import balanced_hybrid_search
from utils import normalize_umkm_label, get_col


# =========================================================
# QUERY UJI
# =========================================================
TEST_QUERIES = [
    "hiasan rumah",
    "kain batik",
    "keripik singkong",
    "kemeja polos pria",
    "tas kulit wanita",
    "kopi khas daerah",
]


# =========================================================
# KONFIGURASI POOLING
# =========================================================
POOL_DEPTH = 50           
TOP_N_CANDIDATES = 2000     
MIN_UMKM_RATIO = 0.4

OUTPUT_FILE = "manual_pool_labeling_template.csv"


# =========================================================
# KONFIGURASI HYBRID UNTUK MEMBENTUK POOL
# =========================================================
POOL_CONFIGS = [
    {"method": "Hybrid_relevance_dominant", "alpha": 0.50, "beta": 0.25, "gamma": 0.25, "lambda_umkm": 0.10},
    {"method": "Hybrid_balanced", "alpha": 0.34, "beta": 0.33, "gamma": 0.33, "lambda_umkm": 0.10},
    {"method": "Hybrid_popularity_dominant", "alpha": 0.25, "beta": 0.50, "gamma": 0.25, "lambda_umkm": 0.10},
    {"method": "Hybrid_value_dominant", "alpha": 0.25, "beta": 0.25, "gamma": 0.50, "lambda_umkm": 0.10},
]


# =========================================================
# FUNCTIONS
# =========================================================

def safe_select_for_labeling(df: pd.DataFrame, query: str, source_method: str) -> list[dict]:
    df = normalize_umkm_label(df).copy()
    rows = []

    for rank, (_, row) in enumerate(df.head(POOL_DEPTH).iterrows(), start=1):
        rows.append({
            "query": query,
            "source_method": source_method,
            "source_rank": rank,

            "id": get_col(row, ["id"], ""),
            "name": get_col(row, ["name"], ""),
            "name_clean": get_col(row, ["name_clean"], ""),
            "category": get_col(row, ["category_breadcrumb"], ""),
            "shop_name": get_col(row, ["shop_name"], ""),
            "shop_city": get_col(row, ["shop_city"], ""),
            "url": get_col(row, ["url"], ""),

            "umkm_label": get_col(row, ["umkm_label"], 0),

            # isi manual: 1 relevan, 0 tidak relevan
            "manual_relevance": "",  
        })

    return rows


# =========================================================
# CREATE POOL
# =========================================================
def create_pool() -> pd.DataFrame:
    all_rows = []

    for query in TEST_QUERIES:
        print(f"\n=== Membuat pool untuk query: {query} ===")

        # 1. BM25 baseline Top-50
        bm25_results = bm25_search(
            query, 
            topk=POOL_DEPTH, 
            use_term_filter=False
        )
        print(f"BM25: {len(bm25_results)} produk")
        all_rows.extend(safe_select_for_labeling(bm25_results, query, "BM25"))

        # 2. Hybrid Top-50 dari beberapa konfigurasi
        for config in POOL_CONFIGS:
            method_name = config["method"]

            hybrid_results = balanced_hybrid_search(
                query=query,
                top_n_candidates=TOP_N_CANDIDATES,
                top_k_results=POOL_DEPTH,
                min_umkm_ratio=MIN_UMKM_RATIO,
                alpha=config["alpha"],
                beta=config["beta"],
                gamma=config["gamma"],
                lambda_umkm=config["lambda_umkm"],
                popularity_sold_weight=0.5,
                popularity_review_weight=0.5,
                value_rating_weight=0.5,
                value_discount_weight=0.5,
            )

            print(f"{method_name}: {len(hybrid_results)} produk")
            all_rows.extend(safe_select_for_labeling(hybrid_results, query, method_name))

    df_raw = pd.DataFrame(all_rows)

    # --- DEDUPLICATION ---
    # jika id tersedia, dedup pakai query+id; jika id kosong, dedup pakai query+name+category.
    df_raw["id_as_str"] = df_raw["id"].fillna("").astype(str).str.strip()

    with_id = df_raw[df_raw["id_as_str"] != ""].drop_duplicates(
        subset=["query", "id_as_str"],
        keep="first"
    )

    without_id = df_raw[df_raw["id_as_str"] == ""].drop_duplicates(
        subset=["query", "name", "category"],
        keep="first"
    )

    df_pool = pd.concat(
        [with_id, without_id],
        ignore_index=True
    )
    
    df_pool = df_pool.drop(columns=["id_as_str"], errors="ignore")

    df_pool = df_pool.sort_values(
        by=["query", "source_method", "source_rank"]
    ).reset_index(drop=True)

    df_pool.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n=================================================")
    print("Template manual labeling berhasil dibuat.")
    print(f"File: {OUTPUT_FILE}")
    print(f"Jumlah baris mentah sebelum dedup: {len(df_raw)}")
    print(f"Jumlah produk unik setelah dedup: {len(df_pool)}")
    print("Isi kolom manual_relevance dengan 1 atau 0.")
    print("Setelah selesai, simpan sebagai manual_pool_labeling_labeled.csv")
    print("=================================================")

    return df_pool


if __name__ == "__main__":
    create_pool()