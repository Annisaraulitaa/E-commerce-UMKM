import base64
import html
import mimetypes

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
            encoded = base64.b64encode(img_file.read()).decode()

        return f"data:{mime_type};base64,{encoded}"
    except Exception:
        return None


def render_product_card(row, rank):
    image_path = get_image_path(row.get("image_local_path"))
    image_src = image_to_base64(image_path)

    umkm_label = str(row.get("umkm_label", "-")).upper()

    if umkm_label == "UMKM":
        badge_color = "#16a34a"
        badge_text = "UMKM"
    else:
        badge_color = "#2563eb"
        badge_text = "NON-UMKM"

    name = html.escape(str(row.get("name", "-")))
    price = format_rp(row.get("price_number", 0))
    rating = row.get("ratingAverage", "-")
    sold = safe_int(row.get("countSold", 0))
    review = safe_int(row.get("countReview", 0))
    shop_city = html.escape(str(row.get("shop_city", "-")))

    if image_src:
        image_html = (
            '<div class="product-image-box">'
            f'<img src="{image_src}" class="product-image">'
            '</div>'
        )
    else:
        image_html = (
            '<div class="product-image-box product-image-empty">'
            'Gambar tidak tersedia'
            '</div>'
        )

    card_html = (
        f'{image_html}'
        '<div class="product-card-body">'
        '<div class="product-card-top">'
        f'<span class="product-badge" style="background:{badge_color};">{badge_text}</span>'
        f'<span class="product-rank">#{rank}</span>'
        '</div>'
        f'<div class="product-name">{name}</div>'
        f'<div class="product-price">{price}</div>'
        f'<div class="product-meta">⭐ {rating} | {review} ulasan</div>'
        f'<div class="product-meta">📦 {sold} terjual</div>'
        f'<div class="product-meta product-location">📍 {shop_city}</div>'
        '</div>'
    )

    with st.container(border=True):
        st.markdown(card_html, unsafe_allow_html=True)

        if st.button(
            "Lihat Detail",
            key=f"detail_{rank}_{row.get('id', rank)}",
            use_container_width=True
        ):
            st.session_state.selected_product = row.to_dict()
            st.session_state.current_page = "Detail Produk"
            st.query_params.clear()
            st.rerun()