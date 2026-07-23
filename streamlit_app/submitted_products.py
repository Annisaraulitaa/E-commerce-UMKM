from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent

SUBMISSION_FILE = (
    BASE_DIR
    / "data"
    / "product_submissions.csv"
)


def _get_series(df, column_name, default=""):
    if column_name in df.columns:
        return df[column_name].fillna(default)

    return pd.Series(
        [default] * len(df),
        index=df.index
    )


def load_approved_submitted_products():
    if not SUBMISSION_FILE.exists():
        return pd.DataFrame()

    submissions = pd.read_csv(SUBMISSION_FILE)

    if submissions.empty or "status" not in submissions.columns:
        return pd.DataFrame()

    submissions["status"] = submissions["status"].astype(str).str.strip().str.lower()

    approved = submissions[
        submissions["status"].eq("approved")
    ].copy()

    if approved.empty:
        return pd.DataFrame()

    # ID khusus produk pendaftaran.
    # Contoh: baris index 4 di CSV menjadi submission_4
    submission_route_id = pd.Series(
        [f"submission_{idx}" for idx in approved.index],
        index=approved.index
    )

    price_raw = (
        _get_series(approved, "estimated_price", 0)
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
    )

    price = pd.to_numeric(
        price_raw,
        errors="coerce"
    ).fillna(0)

    catalog_df = pd.DataFrame({
        "id": submission_route_id.astype(str),
        "submission_id": _get_series(approved, "submission_id", "").astype(str),

        "name": _get_series(approved, "product_name", "").astype(str),
        "url": "",
        "category_breadcrumb": _get_series(approved, "business_category", "").astype(str),

        "price_number": price,
        "price_original": price,

        "ratingAverage": 0,
        "countSold": 0,
        "countReview": 0,
        "totalRating": 0,
        "discountPercentage": 0,

        "shop_name": _get_series(approved, "shop_name", "").astype(str),
        "shop_url": "",
        "shop_city": _get_series(approved, "city", "").astype(str),
        "shop_tier": "Produk Terdaftar",

        "image_local_path": _get_series(approved, "image_local_path", "").astype(str),

        "description": _get_series(approved, "description", "").astype(str),
        "owner_name": _get_series(approved, "owner_name", "").astype(str),
        "email": _get_series(approved, "email", "").astype(str),
        "whatsapp": _get_series(approved, "whatsapp", "").astype(str),
        "province": _get_series(approved, "province", "").astype(str),
        "business_type": _get_series(approved, "business_type", "UMKM").astype(str),

        "umkm_label": 1,
        "umkm_binary": 1,
        "source": "submission",
    })

    return catalog_df.reset_index(drop=True)