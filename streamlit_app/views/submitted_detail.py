import base64
import html
from pathlib import Path
from textwrap import dedent
from urllib.parse import urlencode

import pandas as pd
import streamlit as st

from utils import get_image_path


def _html(parts):
    if isinstance(parts, list):
        markup = "".join(parts)
    else:
        markup = parts

    st.markdown(markup, unsafe_allow_html=True)


def _clean_text(value, default="-"):
    if value is None or pd.isna(value):
        return default

    value = str(value).strip()

    if value.lower() in ["", "nan", "none"]:
        return default

    return value


def _format_price(value):
    try:
        number = float(value)
        if number <= 0:
            return "Harga belum tersedia"

        return f"Rp {int(number):,}".replace(",", ".")
    except Exception:
        return "Harga belum tersedia"


def _whatsapp_url(value):
    phone = "".join(ch for ch in str(value) if ch.isdigit())

    if not phone:
        return ""

    if phone.startswith("0"):
        phone = "62" + phone[1:]

    if not phone.startswith("62"):
        phone = "62" + phone

    return f"https://wa.me/{phone}"


def _image_to_base64(path):
    with open(path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def _image_mime_type(path):
    suffix = Path(path).suffix.lower()

    if suffix in [".jpg", ".jpeg"]:
        return "image/jpeg"

    if suffix == ".webp":
        return "image/webp"

    return "image/png"


def _image_data_uri(path):
    mime_type = _image_mime_type(path)
    encoded_image = _image_to_base64(path)
    return f"data:{mime_type};base64,{encoded_image}"


def _build_back_url():
    params = {"page": "Beranda"}

    last_query = st.query_params.get("q", st.session_state.get("catalog_query", ""))
    last_filter = st.query_params.get(
        "catalog_filter",
        st.session_state.get("catalog_filter", "Semua")
    )

    if last_query:
        params["q"] = last_query

    if last_filter:
        params["catalog_filter"] = last_filter

    return "?" + urlencode(params)


def render_submitted_product_detail(product):
    if not product:
        st.error("Produk tidak ditemukan.")
        return

    name = html.escape(_clean_text(product.get("name"), "Produk Terdaftar"))
    category = html.escape(
        _clean_text(product.get("category_breadcrumb"), "Kategori belum tersedia")
    )
    price = _format_price(product.get("price_number", 0))

    shop_name = html.escape(_clean_text(product.get("shop_name"), "Nama toko belum tersedia"))
    shop_city = html.escape(_clean_text(product.get("shop_city"), "-"))
    province = html.escape(_clean_text(product.get("province"), "-"))

    description = html.escape(
        _clean_text(product.get("description"), "Deskripsi produk belum tersedia.")
    )
    owner_name = html.escape(_clean_text(product.get("owner_name"), "-"))
    email = html.escape(_clean_text(product.get("email"), "-"))
    whatsapp = _clean_text(product.get("whatsapp"), "")
    whatsapp_display = html.escape(whatsapp if whatsapp else "-")
    whatsapp_link = _whatsapp_url(whatsapp)

    image_path = get_image_path(product.get("image_local_path", ""))
    back_url = _build_back_url()

    if image_path is not None:
        image_html = (
            '<div class="submitted-product-image-card" data-component="product-image-card">'
            f'<img class="submitted-product-image" data-component="product-image" '
            f'src="{_image_data_uri(image_path)}" alt="{name}">'
            '</div>'
        )
    else:
        image_html = (
            '<div class="submitted-product-image-card" data-component="product-image-card">'
            '<div class="submitted-product-image-placeholder" data-component="product-image-placeholder">'
            'Foto produk tidak tersedia'
            '</div>'
            '</div>'
        )

    if whatsapp_link:
        contact_button = (
            f'<a class="submitted-action-button submitted-action-primary" '
            f'data-component="contact-seller-button" '
            f'href="{html.escape(whatsapp_link)}" target="_blank" rel="noopener noreferrer">'
            '💬 Hubungi Penjual'
            '</a>'
        )
    else:
        contact_button = (
            '<span class="submitted-action-button submitted-action-disabled" '
            'data-component="contact-seller-button-disabled">'
            'Hubungi Penjual'
            '</span>'
        )

    st.markdown(
        dedent(
            """
            <style>
            /* =========================================================
               SUBMITTED DETAIL PAGE
               Semua komponen utama memakai class stabil agar mudah dicari
               di Inspect Element. Cari prefix: submitted-
               ========================================================= */

            /* ===== HIDE STREAMLIT DEFAULT CHROME ===== */
            header[data-testid="stHeader"],
            div[data-testid="stToolbar"],
            div[data-testid="stDecoration"],
            #MainMenu,
            footer {
                display: none !important;
                height: 0 !important;
            }

            /* ===== STREAMLIT PAGE RESET ===== */
            .stApp {
                background:
                    linear-gradient(rgba(15, 23, 42, 0.42), rgba(15, 23, 42, 0.42)),
                    linear-gradient(135deg, #075985 0%, #0f67b1 45%, #2563eb 100%) !important;
                overflow-x: hidden !important;
            }

            section[data-testid="stMain"] {
                min-height: 100vh !important;
                padding: 0 !important;
            }

            div[data-testid="stMainBlockContainer"],
            .block-container {
                max-width: none !important;
                width: 100% !important;
                margin: 0 !important;
                padding: 0 !important;
            }

            /* ===== PAGE SHELL / WHITE CARD ===== */
            .submitted-page-shell {
                min-height: 100vh;
                box-sizing: border-box;
                display: flex;
                justify-content: center;
                align-items: flex-start;
                padding: 0px 24px 40px 24px;
            }

            .submitted-detail-card {
                width: min(850px, calc(100vw - 220px));
                box-sizing: border-box;
                background: #ffffff;
                border: 1px solid rgba(226, 232, 240, 0.95);
                border-radius: 24px;
                box-shadow: 0 28px 90px rgba(15, 23, 42, 0.26);
                padding: 16px 30px 28px;
            }

            /* ===== BACK BUTTON ===== */
            .submitted-back-row {
                display: flex;
                align-items: center;
                justify-content: flex-start;
                margin-bottom: 15px;
            }

            .submitted-back-button {
                width: auto;
                min-height: 35px;
                box-sizing: border-box;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
                padding: 0px 12px 0px 8px;
                border-radius: 7px;
                background: transparent;
                color: #64748b !important;
                text-decoration: none !important;
                border: 1px solid #e5e7eb;
                font-size: 14px;
                font-weight: 500;
                box-shadow: none;
            }

            .submitted-back-button:hover {
                background: #f1f5f9;
                color: #2563eb !important;
                border-color:#cbd5e1;
            }

            .submitted-back-icon {
                font-size:18px;
                line-height:1;
            }

            .submitted-back-text {
                font-size:14px;
                font-weight:500;
            }

            /* ===== MAIN GRID ===== */
            .submitted-main-grid {
                display: grid;
                grid-template-columns: 1fr 1.2fr;
                gap: 24px;
                align-items: start;
            }

            .submitted-left-column,
            .submitted-right-column {
                min-width: 0;
            }

            /* ===== IMAGE ===== */
            .submitted-product-image-card {
                width: 100%;
                height:340px;
                border-radius: 20px;
                overflow: hidden;
                background: #f8fafc;
                border: 1px solid #e5e7eb;
                display:flex;
                align-items:center;
                justify-content:center;
            }

            .submitted-product-image {
                width: 100%;
                height: 100%;
                object-fit: cover;
                display: block;
            }

            .submitted-product-image-placeholder {
                height: 420px;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #94a3b8;
                font-size: 15px;
                font-weight: 700;
            }

            /* ===== BADGES ===== */
            .submitted-badge-row {
                display: flex;
                gap: 8px;
                margin-bottom: 12px;
                flex-wrap: wrap;
            }

            .submitted-badge {
                display: inline-flex;
                align-items: center;
                border-radius: 999px;
                padding: 7px 14px;
                font-size: 11px;
                font-weight: 900;
                background: #10b981;
                color: #ffffff;
            }

            .submitted-badge-secondary {
                background: #eff6ff;
                color: #2563eb;
                border: 1px solid #bfdbfe;
            }

            /* ===== PRODUCT HEADER ===== */
            .submitted-title {
                font-size: 24px;
                line-height: 1.32;
                color: #0f172a;
                font-weight: 850;
                margin: 0;
                letter-spacing: -0.01em;
            }

            .submitted-category {
                font-size: 13px;
                color: #64748b;
                margin-bottom: 14px;
            }

            /* ===== PRICE ===== */
            .submitted-price-box {
                background: #f8f9ff;
                border-radius: 18px;
                padding: 16px 22px;
                margin-bottom: 14px;
            }

            .submitted-price-label {
                font-size: 13px;
                color: #64748b;
                margin-bottom: 4px;
                font-weight: 700;
            }

            .submitted-price {
                font-size: 32px;
                font-weight: 950;
                color: #2563eb;
                line-height: 1.15;
            }

            /* ===== INFO BOXES ===== */
            .submitted-info-box {
                border: 1px solid #e5e7eb;
                border-radius: 14px;
                padding: 12px 16px;
                margin-bottom: 10px;
                background: #ffffff;
            }

            .submitted-info-title {
                font-size: 14px;
                font-weight: 850;
                color: #0f172a;
                margin-bottom: 8px;
            }

            .submitted-info-text {
                font-size: 13px;
                line-height: 1.6;
                color: #475569;
            }

            .submitted-description-box {
                margin-top: 12px;
                padding:12px 16px;
            }

            /* ===== CONTACT ===== */
            .submitted-contact-grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
                margin-top: 10px;
            }

            .submitted-contact-item {
                background: #f8fafc;
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                padding: 6px 10px;
                font-size: 12px;
                color: #64748b;
            }

            .submitted-contact-label {
                display: block;
                color: #0f172a;
                font-size: 12px;
                font-weight: 850;
                margin-bottom: 3px;
            }

            .submitted-contact-value {
                font-size: 12px;
                color: #64748b;
            }

            /* ===== ACTION BUTTONS ===== */
            .submitted-action-row {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
                margin-top: 12px;
            }

            .submitted-action-button {
                min-height: 40px;
                border-radius: 11px;
                display: flex;
                align-items: center;
                justify-content: center;
                text-decoration: none !important;
                font-size: 14px;
                font-weight: 800;
            }

            .submitted-action-primary {
                background: #2563eb;
                color: #ffffff !important;
            }

            .submitted-action-secondary {
                background: #ffffff;
                color: #2563eb !important;
                border: 1px solid #2563eb;
            }

            .submitted-action-disabled {
                background: #e5e7eb;
                color: #94a3b8 !important;
                border: 1px solid #e5e7eb;
                pointer-events: none;
            }

            @media (max-width: 900px) {
                .submitted-page-shell {
                    padding: 28px 14px 36px 14px;
                }

                .submitted-detail-card {
                    width: 100%;
                    padding: 22px 18px 28px 18px;
                }

                .submitted-main-grid {
                    grid-template-columns: 1fr;
                    gap: 22px;
                }

                .submitted-product-image,
                .submitted-product-image-placeholder {
                    height: 320px;
                }

                .submitted-contact-grid,
                .submitted-action-row {
                    grid-template-columns: 1fr;
                }
            }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )

    detail_html = [
        '<div class="submitted-page-shell" data-component="submitted-page-shell">',
        '<div class="submitted-detail-card" data-component="submitted-detail-card">',

        '<div class="submitted-back-row" data-component="back-row">',
        f'<a class="submitted-back-button" data-component="back-button" href="{html.escape(back_url)}" target="_self">',
        '<span class="submitted-back-icon">‹</span>',
        '<span class="submitted-back-text">Kembali</span>',
        '</a>',
        '</div>',

        '<div class="submitted-main-grid" data-component="main-grid">',

        '<div class="submitted-left-column" data-component="left-column">',
        image_html,
        '<div class="submitted-info-box submitted-description-box" data-component="description-box">',
        '<div class="submitted-info-title" data-component="description-title">Deskripsi Produk</div>',
        f'<div class="submitted-info-text" data-component="description-text">{description}</div>',
        '</div>',
        '</div>',

        '<div class="submitted-right-column" data-component="right-column">',
        '<div class="submitted-badge-row" data-component="badge-row">',
        '<span class="submitted-badge" data-component="umkm-badge">UMKM TERDAFTAR</span>',
        '<span class="submitted-badge submitted-badge-secondary" data-component="verified-badge">Produk Verifikasi Admin</span>',
        '</div>',

        f'<div class="submitted-title" data-component="product-title">{name}</div>',
        f'<div class="submitted-category" data-component="product-category">{category}</div>',

        '<div class="submitted-price-box" data-component="price-box">',
        '<div class="submitted-price-label" data-component="price-label">Estimasi Harga</div>',
        f'<div class="submitted-price" data-component="price-value">{price}</div>',
        '</div>',

        '<div class="submitted-info-box" data-component="shop-info-box">',
        '<div class="submitted-info-title" data-component="shop-info-title">Informasi Toko</div>',
        '<div class="submitted-info-text" data-component="shop-info-text">',
        f'<b>{shop_name}</b><br>',
        f'📍 {shop_city}, {province}',
        '</div>',
        '</div>',

        '<div class="submitted-info-box" data-component="contact-box">',
        '<div class="submitted-info-title" data-component="contact-title">Kontak Penjual</div>',
        '<div class="submitted-contact-grid" data-component="contact-grid">',

        '<div class="submitted-contact-item" data-component="email-card">',
        '<span class="submitted-contact-label">Email</span>',
        f'<span class="submitted-contact-value">{email}</span>',
        '</div>',

        '<div class="submitted-contact-item" data-component="whatsapp-card">',
        '<span class="submitted-contact-label">WhatsApp</span>',
        f'<span class="submitted-contact-value">{whatsapp_display}</span>',
        '</div>',

        '</div>',
        '</div>',

        '<div class="submitted-action-row" data-component="action-row">',
        contact_button,
        f'<a class="submitted-action-button submitted-action-secondary" data-component="other-products-button" href="{html.escape(back_url)}" target="_self">Lihat Produk Lain</a>',
        '</div>',

        '</div>',
        '</div>',
        '</div>',
        '</div>',
    ]

    _html(detail_html)