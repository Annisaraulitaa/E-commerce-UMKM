import base64
import html
import mimetypes

import streamlit as st

from utils import format_rp, get_image_path, get_value, safe_int


def _clean_url(value):
    if value is None:
        return ""

    value = str(value).strip()

    if value in ["", "-"]:
        return ""

    return value


def _html(parts):
    st.markdown("".join(parts), unsafe_allow_html=True)


def _image_to_data_uri(image_path):
    """Ubah file gambar lokal menjadi data URI agar aman dirender di browser Streamlit."""
    if image_path is None:
        return ""

    mime_type, _ = mimetypes.guess_type(str(image_path))
    if mime_type is None:
        mime_type = "image/jpeg"

    with open(image_path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    return f"data:{mime_type};base64,{encoded}"


def format_sold(value):
    n = safe_int(value)

    if n >= 1_000_000:
        val = n / 1_000_000
        if val.is_integer():
            return f"{int(val)}jt+"
        return f"{val:.1f}".replace(".", ",") + "jt+"

    if n >= 1_000:
        val = n / 1_000
        if val.is_integer():
            return f"{int(val)}rb+"
        return f"{val:.1f}".replace(".", ",") + "rb+"

    return str(n)


def render_detail_product(product):
    if product is None:
        st.warning("Produk belum dipilih.")
        return

    image_path = get_image_path(product.get("image_local_path"))

    name = html.escape(str(get_value(product, "name", default="-")))
    product_url = _clean_url(get_value(product, "url", default=""))
    category = html.escape(str(get_value(product, "category_breadcrumb", default="-")))

    price_raw = get_value(product, "price_number", default=0)
    original_price_raw = get_value(product, "price_original", default=0)

    price = format_rp(price_raw)
    original_price = format_rp(original_price_raw)
    discount = safe_int(get_value(product, "discountPercentage", default=0))

    rating = get_value(product, "ratingAverage", default="-")
    sold = safe_int(get_value(product, "countSold", default=0))
    sold_text = format_sold(sold)
    total_rating = safe_int(get_value(product, "totalRating", default=0))
    review = safe_int(get_value(product, "countReview", default=0))

    shop_name = html.escape(str(get_value(product, "shop_name", default="-")))
    shop_url = _clean_url(get_value(product, "shop_url", default=""))
    shop_city = html.escape(str(get_value(product, "shop_city", default="-")))
    shop_tier = html.escape(str(get_value(product, "shop_tier", default="-")))

    umkm_label = html.escape(str(get_value(product, "umkm_label", default="-")).upper())
    badge_color = "#00c951" if umkm_label == "UMKM" else "#2b7fff"

    try:
        price_num = int(float(price_raw))
        original_price_num = int(float(original_price_raw))
        saving = max(0, original_price_num - price_num)
    except Exception:
        saving = 0

    saving_text = format_rp(saving) if saving > 0 else "-"

    st.markdown(
        """
        <style>
        /* ===== FLOATING MODAL PAGE BACKGROUND ===== */
        .stApp {
            background: #edf4ff !important;
            position: relative;
            overflow-x: hidden;
        }

        .stApp::before {
            content: "";
            position: fixed;
            inset: 0;
            z-index: 0;
            pointer-events: none;
            background:
                linear-gradient(rgba(248, 250, 252, 0.82), rgba(248, 250, 252, 0.82)),
                radial-gradient(circle at 18% 18%, rgba(37, 99, 235, 0.16), transparent 28%),
                radial-gradient(circle at 84% 22%, rgba(22, 163, 74, 0.14), transparent 26%),
                radial-gradient(circle at 50% 90%, rgba(99, 102, 241, 0.12), transparent 32%),
                repeating-linear-gradient(0deg, rgba(15, 23, 42, 0.035) 0px, rgba(15, 23, 42, 0.035) 1px, transparent 1px, transparent 96px),
                repeating-linear-gradient(90deg, rgba(15, 23, 42, 0.035) 0px, rgba(15, 23, 42, 0.035) 1px, transparent 1px, transparent 220px);
            filter: blur(1.6px);
            transform: scale(1.03);
        }

        header[data-testid="stHeader"] {
            height: 0px !important;
            visibility: hidden !important;
        }

        div[data-testid="stAppViewContainer"] {
            padding-top: 0rem !important;
            position: relative;
            z-index: 1;
        }

        /* Floating card utama */
        section[data-testid="stMain"] {
            min-height: 100vh !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
        }

        .block-container {
            max-width: 1000px !important;
            width: calc(100% - 64px) !important;
            min-height: 620px !important;
            margin: 24px auto 32px auto !important; 
            padding: 24px 48px 36px 48px !important;
            background: rgba(255, 255, 255, 0.94) !important;
            border: 1px solid rgba(226, 232, 240, 0.95) !important;
            border-radius: 16px !important;
            box-shadow: 0 28px 90px rgba(15, 23, 42, 0.20) !important;
            backdrop-filter: blur(18px);
            -webkit-backdrop-filter: blur(18px);
        }

        body {
            background: #edf4ff;
        }

        div[data-testid="stElementContainer"]:has(div[data-testid="stButton"]) {
            margin-bottom: 12px !important;
            padding-bottom: 0px !important;
        }

        div[data-testid="stVerticalBlock"] {
            gap: 0rem !important;
        }

        div[data-testid="stHorizontalBlock"] {
            margin-bottom: 0px !important;
        }

        div[data-testid="column"] {
            padding-top: 0px !important;
        }

        .element-container {
            margin-bottom: 0rem !important;
        }

        /* ===== BACK BUTTON ===== */
        div[data-testid="stButton"] {
            /* width: fit-content !important; */
            /* margin-bottom: 8px !important; */
            width: fit-content !important;
            margin: 0 0 16px 0 !important;
            padding: 0px !important;
        }

        div[data-testid="stButton"] button {
            /* width: auto !important; */
            /* min-width: 40px !important; */
            /* height: 40px !important; */
            /* min-height: 40px !important; */
            /* padding: 0px 0px !important; */
            /* border-radius: 10px !important; */
            /* border: 1px solid #dbe3ef !important; */
            /* background: #ffffff !important; */
            /* color: #2563eb !important; */
            /* font-size: 12px !important; */
            /* font-weight: 650 !important; */
            /* box-shadow: none !important; */
            /* transition: all 0.18s ease !important; */
            width: auto !important;
            min-width: 70px !important;
            height: 40px !important;
            min-height: 40px !important;
            padding: 0px 10px 0px 8px !important;

            display: inline-flex !important;
            flex-direction: row !important;
            align-items: center !important;
            justify-content: center !important;
            gap: 6px !important;
            flex-wrap: nowrap !important;

            border: 1px solid #e5e7eb !important;
            border-radius: 7px !important;
            background: transparent !important;

            color: #64748b !important;
            box-shadow: none !important;
            transition: all 150ms cubic-bezier(0.4, 0, 0.2, 1) !important;
        }

        /* Ukuran tulisan "kembali" */
        div[data-testid="stButton"] button p {
            font-size: 14px !important;
            font-weight: 500 !important;
            line-height: 1 !important;
            margin: 0 !important;
            padding: 0 !important;
        }

        /* Ukuran icon material chevron_left */
        div[data-testid="stButton"] button span[data-testid="stIconMaterial"],
        div[data-testid="stButton"] button span.material-symbols-rounded {
            font-size: 18px !important;
            width: 18px !important;
            height: 18px !important;
            line-height: 1 !important;
            flex-shrink: 0 !important;
        }

        div[data-testid="stButton"] button:hover {
            /* background: #2563eb !important; */
            /* color: #ffffff !important; */
            /* border-color: #2563eb !important; */
            /* transform: translateY(-1px); */
            background: #f1f5f9 !important;
            border-color: #cbd5e1 !important;
            color: #2563eb !important;
            transform: translateY(-1px);
        }

        div[data-testid="stButton"] button:active {
            transform: translateY(0px);
        }

        /* ===== MARKETPLACE DETAIL LAYOUT ===== */
        .detail-divider {
            height: 1px;
            background: #e5e7eb;
            margin: 8px 0 18px 0;
        } 

        .product-image-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 0px;
            padding: 3px;
            min-height: auto;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
        }

        .product-image-card img {
            width: 100%;
            max-height: 700px;
            object-fit: cover;
            border-radius: 0px;
            display: block;
        }

        .detail-empty-image {
            min-height: 445px;
            background: #f1f5f9;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #94a3b8;
            font-weight: 700;
        }

        .detail-info-wrap {
            padding-top: 0px;
        }

        .detail-badge-row {
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: nowrap;
            margin-bottom: 7px;
            max-width: 100%;
            overflow: hidden;
        }

        .detail-badge {
            color: white;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 11px;
            font-weight: 850;
            display: inline-flex;
            align-items: center;
            white-space: nowrap;
            flex: 0 0 auto;
        }

        .detail-title {
            font-size: 27px;
            line-height: 1.32;
            color: #0f172a;
            font-weight: 850;
            margin-bottom: 8px;
            letter-spacing: -0.01em;
        }

        .detail-meta {
            font-size: 14px;
            color: #334155;
            margin-bottom: 24px;
        }

        .detail-price-box {
            background: #f8f9ff;
            border: none;
            border-radius: 18px;
            padding: 20px 22px;
            margin-bottom: 28px;
            box-shadow: none;
        }

        .detail-original-price {
            font-size: 14px;
            color: #94a3b8;
            text-decoration: line-through;
            margin-bottom: 5px;
        }

        .detail-price {
            font-size: 31px;
            font-weight: 950;
            color: #2563eb;
            line-height: 1.15;
            margin: 4px 0px;
        }

        .detail-saving {
            font-size: 13px;
            color: #16a34a;
            font-weight: 800;
        }

        .shop-box {
            border: 1px solid #e5e7eb;
            border-radius: 10px;
            padding: 10px 16px;
            margin-bottom: 28px;
            background: #ffffff;
        }

        .shop-name {
            font-size: 15px;
            font-weight: 550;
            color: #0f172a;
            margin-bottom: 0px;
            padding: 0px 0px 0px 3px;
        }

        .shop-location {
            color: #64748b;
            font-size: 12px;
        }

        .action-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
            margin-top: 2px;
        }

        .market-action {
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 44px;
            border: 1px solid #d1d5db;
            border-radius: 11px;
            background: #ffffff;
            color: #0f172a !important;
            font-size: 15px;
            font-weight: 700;
            text-decoration: none !important;
            transition: all 0.18s ease;
        }

        .market-action:hover {
            border-color: #2563eb;
            color: #2563eb !important;
            background: #eff6ff;
        }

        .market-action.disabled {
            color: #94a3b8 !important;
            cursor: not-allowed;
            background: #f8fafc;
        }


        .bottom-card-space {
            height: 32px;
            width: 100%;
        }

        .image-discount-note {
            margin-top: 8px;
            padding: 0 4px 12px 4px;
            color: #334155;
            line-height: 1.45;
            font-size: 14px;
        }

        .image-discount-note p {
            margin: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Kembali", icon=":material/chevron_left:"):
        st.session_state.current_page = "Katalog Produk"
        st.query_params["page"] = "Katalog Produk"
        st.rerun()


    #_html(['<div class="detail-divider"></div>'])

    discount_note_html = ""

    if discount > 0 and original_price != "Rp 0" and original_price != price:
        discount_note_html = (
            f'<p>'
            f'Produk ini memiliki diskon sebesar <b>{discount}%</b> '
            f'dari harga awal produk sebesar <b>{original_price}</b>.'
            f'</p>'
        )

    col_img, col_info = st.columns([1.00, 1.20], gap="medium")

    with col_img:
        if image_path is not None:
            try:
                image_src = _image_to_data_uri(image_path)
                image_parts = [
                    '<div class="product-image-card">',
                    f'<img src="{image_src}" alt="{name}">',
                    '</div>',
                ]

                if discount_note_html:
                    image_parts.extend([
                        '<div class="image-discount-note">',
                        discount_note_html,
                        '</div>',
                    ])

                _html(image_parts)
            except Exception:
                st.image(str(image_path), use_container_width=True)
                if discount_note_html:
                    _html([
                        '<div class="image-discount-note">',
                        discount_note_html,
                        '</div>',
                    ])
        else:
            empty_parts = [
                '<div class="detail-empty-image">',
                'Gambar tidak tersedia',
                '</div>',
            ]

            if discount_note_html:
                empty_parts.extend([
                    '<div class="image-discount-note">',
                    discount_note_html,
                    '</div>',
                ])

            _html(empty_parts)

    with col_info:
        _html([
            '<div class="detail-info-wrap">',
            '<div class="detail-badge-row">',
            f'<span class="detail-badge" style="background:{badge_color};">{umkm_label}</span>',
            '</div>',
            '<div class="detail-title">',
            name,
            '</div>',
            '<div class="detail-meta">',
            f'⭐ {rating} &nbsp; | &nbsp; {review} ulasan &nbsp; | &nbsp;📦 {sold_text} terjual',
            '</div>',
        ])

        price_parts = ['<div class="detail-price-box">']

        if original_price != "Rp 0" and original_price != price:
            price_parts.append(f'<div class="detail-original-price">{original_price}</div>')

        price_parts.append(f'<div class="detail-price">{price}</div>')

        if saving > 0:
            price_parts.append(f'<div class="detail-saving">Hemat {saving_text}</div>')
        elif discount > 0:
            price_parts.append(f'<div class="detail-saving">Diskon {discount}%</div>')

        price_parts.append('</div>')
        _html(price_parts)

        _html([
            '<div class="shop-box">',
            f'<div class="shop-name">{shop_name}</div>',
            f'<div class="shop-location">📍 {shop_city} <span style="color:#94a3b8;">(Tier toko: {shop_tier})</span></div>',
            '</div>',
        ])

        buy_button = (
            f'<a class="market-action" href="{html.escape(product_url)}" target="_blank" rel="noopener noreferrer">Beli Sekarang</a>'
            if product_url
            else '<span class="market-action disabled">Beli Sekarang</span>'
        )
        shop_button = (
            f'<a class="market-action" href="{html.escape(shop_url)}" target="_blank" rel="noopener noreferrer">Kunjungi Toko</a>'
            if shop_url
            else '<span class="market-action disabled">Kunjungi Toko</span>'
        )

        _html([
            '<div class="action-row">',
            buy_button,
            shop_button,
            '</div>',
            '</div>',
        ])

    # Space kosong bawah agar jarak konten ke batas bawah card proporsional.
    _html(['<div class="bottom-card-space"></div>'])
