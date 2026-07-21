import re
from pathlib import Path

import pandas as pd

from config import PROJECT_DIR, BASE_DIR


def get_image_path(image_local_path):
    if pd.isna(image_local_path) or str(image_local_path).strip() == "":
        return None

    raw_path = str(image_local_path).strip().replace("\\", "/")

    # 1. Kalau path sudah absolute, langsung cek
    absolute_path = Path(raw_path)
    if absolute_path.exists():
        return absolute_path

    # 2. Untuk gambar hasil upload dari form pendaftaran
    # Contoh: data/submitted_product_images/namafile.jpg
    candidate_0 = BASE_DIR / raw_path
    if candidate_0.exists():
        return candidate_0

    # 3. Untuk gambar dataset utama lama
    candidate_1 = PROJECT_DIR / raw_path
    if candidate_1.exists():
        return candidate_1

    candidate_2 = PROJECT_DIR / "olahData" / raw_path
    if candidate_2.exists():
        return candidate_2

    candidate_3 = PROJECT_DIR / "output" / Path(raw_path).name
    if candidate_3.exists():
        return candidate_3

    return None


def format_rp(value):
    if pd.isna(value):
        return "-"

    if isinstance(value, str):
        digits = re.sub(r"[^0-9]", "", value)
        if digits == "":
            return value
        value = int(digits)

    try:
        return f"Rp {int(float(value)):,}".replace(",", ".")
    except Exception:
        return "-"


def safe_int(value):
    try:
        if pd.isna(value):
            return 0
        return int(float(value))
    except Exception:
        return 0


def safe_float(value, digits=4):
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), digits)
    except Exception:
        return 0.0


def get_value(row, *keys, default="-"):
    for key in keys:
        value = row.get(key)
        if value is not None and not pd.isna(value) and str(value).strip() != "":
            return value
    return default


# ==============================
# Product Submission Utilities
# ==============================

SUBMISSION_PATH = BASE_DIR / "data" / "product_submissions.csv"


def load_submissions():

    if not SUBMISSION_PATH.exists():
        return pd.DataFrame()

    return pd.read_csv(
        SUBMISSION_PATH
    )


def update_submission_status(index, new_status):

    df = load_submissions()

    df.loc[index, "status"] = new_status

    df.to_csv(
        SUBMISSION_PATH,
        index=False,
        encoding="utf-8-sig"
    )

    return df


def get_approved_submissions():

    df = load_submissions()

    if df.empty:
        return pd.DataFrame()

    return df[
        df["status"] == "approved"
    ].copy()


def delete_submission(index):

    df = load_submissions()

    if index in df.index:

        df = df.drop(index)

        df.to_csv(
            SUBMISSION_PATH,
            index=False,
            encoding="utf-8-sig"
        )

    return df