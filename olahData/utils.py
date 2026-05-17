import re
import pandas as pd


def normalize_umkm_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "umkm_label" not in df.columns:
        df["umkm_label"] = 0

    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0,
    })

    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)

    return df


def get_col(row, columns, default=""):
    for col in columns:
        if col in row.index:
            value = row.get(col, default)
            if pd.notna(value) and str(value).strip() != "":
                return value
            
    return default


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""

    text = str(value).lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_id(value) -> str:
    """
    Menormalkan id produk.
    Catatan:
    Excel kadang mengubah id panjang menjadi scientific notation.
    Karena itu matching tidak hanya mengandalkan id, tetapi juga url dan text key.
    """
    if pd.isna(value):
        return ""

    value = str(value).strip()

    if value.lower() in ["", "nan", "none"]:
        return ""

    # Jika terbaca sebagai 12345.0, ubah menjadi 12345
    if value.endswith(".0"):
        value = value[:-2]

    # Normalisasi decimal comma pada scientific notation Excel Indonesia.
    # Contoh: 1,00315E+11 -> 1.00315E+11
    if "e" in value.lower() and "," in value:
        try:
            value_float = float(value.replace(",", "."))
            value = str(int(value_float))
        except Exception:
            pass

    return value


def normalize_url(value) -> str:
    if pd.isna(value):
        return ""

    url = str(value).strip().lower()

    if not url or url == "nan":
        return ""

    # Buang query string agar URL yang sama tetap match walaupun extParam berbeda
    url = url.split("?")[0]
    return url


def get_candidate_keys(row) -> list:
    """
    Menghasilkan beberapa kemungkinan key untuk 1 produk:
    1. id key
    2. url key
    3. text key = name + category

    Ini penting karena file manual yang diedit di Excel kadang mengubah id.
    Dengan multi-key, peluang produk hasil sistem cocok dengan label manual lebih besar.
    """
    keys = []

    product_id = normalize_id(get_col(row, ["id"]))
    if product_id:
        keys.append(f"id::{product_id}")

    url = normalize_url(get_col(row, ["url"]))
    if url:
        keys.append(f"url::{url}")

    name = normalize_text(get_col(row, ["name"]))
    category = normalize_text(get_col(row, ["category", "category_breadcrumb"]))
    if name or category:
        keys.append(f"text::{name}::{category}")

    # Hilangkan duplikat, tetap pertahankan urutan
    unique_keys = []
    seen = set()
    for key in keys:
        if key not in seen:
            unique_keys.append(key)
            seen.add(key)

    return unique_keys


def get_primary_product_key(row) -> str:
    keys = get_candidate_keys(row)
    return keys[0] if keys else ""