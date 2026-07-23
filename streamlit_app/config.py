from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent


# =========================================================
# DATASET CONFIG
# =========================================================

DATA_ID = "1Eqk9BqquJDhzBr9kxLoibg1DxsyBr9YS"

DATA_PATH_LOCAL = "nondup_dataset.csv"


# =========================================================
# DISPLAY CONFIG
# =========================================================
INITIAL_DISPLAY = 60
LOAD_MORE_STEP = 60


# =========================================================
# RETRIEVAL CONFIG
# Kandidat hasil BM25 sebelum proses ranking/filter
# Berdasarkan hasil eksperimen:
# relevance_dominant + Top-K 60
# =========================================================
TOP_N_CANDIDATES = 2000


# =========================================================
# SEARCH RESULT CONFIG
# =========================================================
# Kandidat yang dikirim ke halaman katalog
SEARCH_POOL_SIZE = 300
# Jumlah rekomendasi final
TOP_K_RESULTS = 60


# =========================================================
# MAIN HYBRID WEIGHTS
# =========================================================
WEIGHT_RELEVANCE = 0.50
WEIGHT_POPULARITY = 0.25
WEIGHT_VALUE = 0.25


# =========================================================
# POPULARITY INTERNAL WEIGHTS
# =========================================================
POPULARITY_SOLD_WEIGHT = 0.50
POPULARITY_REVIEW_WEIGHT = 0.25
POPULARITY_TOTAL_RATING_WEIGHT = 0.25


# =========================================================
# VALUE INTERNAL WEIGHTS
# =========================================================
VALUE_RATING_WEIGHT = 0.70
VALUE_DISCOUNT_WEIGHT = 0.30


# =========================================================
# BM25 DEFAULT PARAMETER
# =========================================================
BM25_K1 = 1.5
BM25_B = 0.75


# =========================================================
# PAGE CONFIG
# =========================================================
VALID_PAGES = [
    "Beranda",
    "Tentang",
    "Admin",
    "Detail Produk",
    "SubmittedDetail"
]