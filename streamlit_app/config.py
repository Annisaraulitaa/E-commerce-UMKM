from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

DATA_PATH = PROJECT_DIR / "olahData" / "retrieval" / "nondup_labeled_dataset(new).csv"

INITIAL_DISPLAY = 60
LOAD_MORE_STEP = 60

WEIGHT_RELEVANCE = 0.50
WEIGHT_POPULARITY = 0.20
WEIGHT_VALUE = 0.20
WEIGHT_UMKM = 0.10

VALID_PAGES = [
    "Beranda",
    "Katalog Produk",
    "Tentang",
]