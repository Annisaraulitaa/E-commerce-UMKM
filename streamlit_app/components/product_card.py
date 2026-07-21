import base64
import html
import mimetypes
from urllib.parse import quote

import pandas as pd
import streamlit as st

from utils import format_rp, get_image_path, safe_int


def image_to_base64(image_path):
    if image_path is None:
        return None

    try:
        mime_type, _ = mimetypes.guess_type(str(image_path))

        if mime_type is None:
            mime_type = "image/jpeg"

        with open(image_path, "rb") as img_file:
            encoded = base64.b64encode(img_file.read()).decode("utf-8")

        return f"data:{mime_type};base64,{encoded}"
    except Exception:
        return None


def placeholder_image():
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="800" height="800" viewBox="0 0 800 800">'
        '<rect width="800" height="800" fill="#f1f5f9"/>'
        '<rect x="120" y="150" width="560" height="360" rx="26" fill="#e2e8f0"/>'
        '<circle cx="300" cy="280" r="52" fill="#cbd5e1"/>'
        '<path d="M165 480 L335 340 L455 440 L535 365 L635 480 Z" fill="#cbd5e1"/>'
        '<text x="400" y="610" text-anchor="middle" font-family="Arial" font-size="34" font-weight="700" fill="#475569">'
        'Gambar Produk'
        '</text>'
        '<text x="400" y="660" text-anchor="middle" font-family="Arial" font-size="24" fill="#64748b">'
        'Tidak tersedia'
        '</text>'
        '</svg>'
    )

    encoded = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


def format_sold(value):
    n = safe_int(value)

    if n >= 1_000_000:
        val = n / 1_000_000
        return f"{val:.1f}".replace(".0", "").replace(".", ",") + "jt+"

    if n >= 1_000:
        val = n / 1_000
        return f"{val:.1f}".replace(".0", "").replace(".", ",") + "rb+"

    return str(n)


def get_category_text(row):
    category = row.get("category_breadcrumb", row.get("categoryBreadcrumbs", "-"))

    if isinstance(category, list):
        category = " › ".join([str(item) for item in category])

    category = str(category)

    if category.strip() == "" or category.lower() == "nan":
        return "-"

    parts = [
        part.strip()
        for part in category.replace(">", "›").split("›")
        if part.strip()
    ]

    return " › ".join(parts[:3]) if parts else "-"


def render_product_card(row, rank, key_prefix="product"):
    product_id = quote(str(row.get("id", rank)))

    current_query = st.session_state.get("catalog_query", "")
    current_filter = st.session_state.get("catalog_filter", "Semua")

    detail_url = (
        f"?page=Beranda"
        f"&product_id={product_id}"
        f"&q={quote(str(current_query))}"
        f"&catalog_filter={quote(str(current_filter))}"
    )

    image_path = get_image_path(row.get("image_local_path"))
    image_src = image_to_base64(image_path) or placeholder_image()

    try:
        umkm_label = int(row.get("umkm_label", 0))
    except:
        umkm_label = 0

    if umkm_label == 1:
        badge_class = "umkm"
        badge_text = "UMKM"
    else:
        badge_class = "non"
        badge_text = "NON-UMKM"

    name = html.escape(str(row.get("name", "-")))
    category = html.escape(get_category_text(row))

    price_raw = row.get("price_number", row.get("price", 0))
    price = format_rp(price_raw)

    original_raw = row.get("price_original", 0)
    original_price = format_rp(original_raw)

    discount = safe_int(row.get("discountPercentage", 0))
    sold = format_sold(row.get("countSold", 0))
    rating = row.get("ratingAverage", "-")

    shop_name = html.escape(str(row.get("shop_name", "-")))
    shop_city = html.escape(str(row.get("shop_city", "-")))

    try:
        price_num = int(float(price_raw))
        original_num = int(float(original_raw))
        show_original = original_num > price_num
    except Exception:
        show_original = False

    original_html = ""

    if show_original:
        original_html = f'<div class="market-original-price">{original_price}</div>'

    discount_html = ""

    if discount > 0:
        discount_html = f'<span class="market-discount">-{discount}%</span>'

    card_parts = [
        f'<a class="market-card-link" href="{detail_url}" target="_self">',
        '<div class="market-card">',

        '<div class="market-image-wrap">',
        f'<img src="{image_src}" class="market-image" alt="{name}">',
        f'<span class="market-badge {badge_class}">{badge_text}</span>',
        discount_html,
        '</div>',

        '<div class="market-card-body">',
        f'<div class="market-category">{category}</div>',
        f'<div class="market-name">{name}</div>',

        '<div class="market-price-wrap">',
        original_html,
        f'<div class="market-price">{price}</div>',
        '</div>',

        '<div class="market-rating-row">',
        '<span class="market-star">★</span>',
        f'<span>{rating}</span>',
        '<span class="market-muted">·</span>',
        f'<span class="market-muted">{sold} terjual</span>',
        '</div>',

        f'<div class="market-shop">{shop_name} · {shop_city}</div>',
        '</div>',

        '</div>',
        '</a>',
    ]

    st.markdown("".join(card_parts), unsafe_allow_html=True)