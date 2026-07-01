from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

DATA_PATH = PROJECT_DIR / "olahData" / "retrieval" / "nondup_labeled_dataset(new).csv"


# =========================================================
# DISPLAY CONFIG
# =========================================================
INITIAL_DISPLAY = 60
LOAD_MORE_STEP = 60


# =========================================================
# FINAL HYBRID RECOMMENDATION CONFIG
# Berdasarkan hasil eksperimen:
# relevance_dominant + lambda 0.30 + Top-K 60
# =========================================================
TOP_N_CANDIDATES = 2000
TOP_K_RESULTS = 60
FIRST_UMKM_QUOTA = 60


# =========================================================
# MAIN HYBRID WEIGHTS
# =========================================================
WEIGHT_RELEVANCE = 0.50
WEIGHT_POPULARITY = 0.25
WEIGHT_VALUE = 0.25
WEIGHT_UMKM = 0.20


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
# PAGE CONFIG
# =========================================================
VALID_PAGES = [
    "Beranda",
    "Tentang",
]