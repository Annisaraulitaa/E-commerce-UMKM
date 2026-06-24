import re
from pathlib import Path

import pandas as pd

from config import PROJECT_DIR


def get_image_path(image_local_path):
    if pd.isna(image_local_path) or str(image_local_path).strip() == "":
        return None

    raw_path = str(image_local_path).strip().replace("\\", "/")

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