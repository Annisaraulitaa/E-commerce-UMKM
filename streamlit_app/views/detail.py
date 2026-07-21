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


def _placeholder_image_data_uri(name="-", category="-"):
    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="800" height="800" viewBox="0 0 800 800">
        <rect width="800" height="800" rx="36" fill="#f8fafc"/>
        <rect x="90" y="120" width="620" height="420" rx="28" fill="#e2e8f0"/>
        <circle cx="285" cy="260" r="58" fill="#cbd5e1"/>
        <path d="M150 500 L335 340 L450 445 L535 365 L650 500 Z" fill="#cbd5e1"/>
        <text x="400" y="610" text-anchor="middle" font-family="Arial, sans-serif" font-size="34" font-weight="700" fill="#334155">
            Gambar Produk
        </text>
        <text x="400" y="660" text-anchor="middle" font-family="Arial, sans-serif" font-size="24" fill="#64748b">
            Tidak tersedia
        </text>
    </svg>
    """

    encoded = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


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

    raw_umkm_label = get_value(
        product,
        "umkm_label",
        default=0
    )

    try:
        is_umkm = int(float(raw_umkm_label)) == 1
    except Exception:
        is_umkm = False


    umkm_label = "UMKM" if is_umkm else "NON-UMKM"

    badge_color = (
        "#00c951"
        if is_umkm
        else "#2b7fff"
    )

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
            background:
                linear-gradient(rgba(15, 23, 42, 0.42), rgba(15, 23, 42, 0.42)),
                linear-gradient(135deg, #075985 0%, #0f67b1 45%, #2563eb 100%) !important;
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
                radial-gradient(circle at 18% 18%, rgba(255, 255, 255, 0.16), transparent 30%),
                radial-gradient(circle at 82% 18%, rgba(255, 255, 255, 0.10), transparent 28%),
                rgba(15, 23, 42, 0.18);
            backdrop-filter: blur(2px);
            -webkit-backdrop-filter: blur(2px);
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
            max-width: 920px !important;
            width: calc(100% - 180px) !important;
            min-height: auto !important;
            margin: 20px auto 34px auto !important; 
            padding: 22px 36px 30px 36px !important;

            background: #ffffff !important;
            border: 1px solid rgba(226, 232, 240, 0.95) !important;
            border-radius: 24px !important;
            box-shadow: 0 28px 90px rgba(15, 23, 42, 0.26) !important;

            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
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
            width: fit-content !important;
            margin: 0 0 16px 0 !important;
            padding: 0px !important;
        }

        div[data-testid="stButton"] button {
            width: auto !important;
            min-width: 70px !important;
            height: 35px !important;
            min-height: 35px !important;
            padding: 0px 12px 0px 5px !important;

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
            border-radius: 5px;
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
            font-size: 24px;
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
            border: 1px solid #93c5fd;
            border-radius: 11px;
            background: #ffffff;
            color: #2563eb !important;
            font-size: 15px;
            font-weight: 700;
            text-decoration: none !important;
            transition: all 0.18s ease;
        }
        
        .market-action:hover,
        .market-action:focus,
        .market-action:active {
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
        
        /* ===== FINAL OVERRIDE DETAIL PRODUCT RIGHT CONTENT ===== */
        section[data-testid="stMain"] .block-container .detail-info-wrap {
            padding-top: 0px !important;
        }

        section[data-testid="stMain"] .block-container .detail-badge-row {
            margin-bottom: 1px !important;
        }

        section[data-testid="stMain"] .block-container .detail-badge {
            padding: 7px 14px !important;
            border-radius: 999px !important;
            font-size: 11px !important;
            font-weight: 850 !important;
        }

        section[data-testid="stMain"] .block-container .detail-title {
            font-size: 23px !important;
            line-height: 1.32 !important;
            color: #0f172a !important;
            font-weight: 700 !important;
            margin-bottom: 10px !important;
            letter-spacing: -0.01em !important;
        }

        section[data-testid="stMain"] .block-container .detail-meta {
            font-size: 13px !important;
            color: #334155 !important;
            margin-bottom: 20px !important;
        }

        section[data-testid="stMain"] .block-container .detail-price-box {
            background: #f8f9ff !important;
            border-radius: 18px !important;
            padding: 12px 22px !important;
            margin-bottom: 26px !important;
            box-shadow: none !important;
        }

        section[data-testid="stMain"] .block-container .detail-original-price {
            font-size: 14px !important;
            color: #94a3b8 !important;
            text-decoration: line-through !important;
            margin-bottom: 4px !important;
        }

        section[data-testid="stMain"] .block-container .detail-price {
            font-size: 30px !important;
            font-weight: 950 !important;
            color: #2563eb !important;
            line-height: 1.15 !important;
            margin: 4px 0 -1px 0 !important;
        }

        section[data-testid="stMain"] .block-container .detail-saving {
            font-size: 13px !important;
            color: #16a34a !important;
            font-weight: 800 !important;
        }

        section[data-testid="stMain"] .block-container .shop-box {
            border: 1px solid #e5e7eb !important;
            border-radius: 14px !important;
            padding: 14px 18px !important;
            margin-bottom: 19px !important;
            background: #ffffff !important;
        }

        section[data-testid="stMain"] .block-container .shop-name {
            font-size: 15px !important;
            font-weight: 550 !important;
            color: #0f172a !important;
            margin-bottom: 3px !important;
            padding: 0 !important;
        }

        section[data-testid="stMain"] .block-container .shop-location {
            color: #64748b !important;
            font-size: 12px !important;
        }

        section[data-testid="stMain"] .block-container .action-row {
            display: grid !important;
            grid-template-columns: 1fr 1fr !important;
            gap: 18px !important;
            margin: 18px 0 !important;
        }

        section[data-testid="stMain"] .block-container .market-action {
            min-height: 44px !important;
            border-radius: 11px !important;
            font-size: 15px !important;
            font-weight: 750 !important;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Kembali", icon=":material/chevron_left:"):
        st.session_state.current_page = "Beranda"
        st.session_state.selected_product = None

        last_query = st.query_params.get("q", st.session_state.get("catalog_query", ""))
        last_filter = st.query_params.get("catalog_filter", st.session_state.get("catalog_filter", "Semua"))

        st.query_params.clear()
        st.query_params["page"] = "Beranda"

        if last_query:
            st.query_params["q"] = last_query
            st.session_state.catalog_query = last_query

        if last_filter:
            st.query_params["catalog_filter"] = last_filter
            st.session_state.catalog_filter = last_filter

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
        try:
            if image_path is not None:
                image_src = _image_to_data_uri(image_path)
            else:
                image_src = _placeholder_image_data_uri(name, category)
        except Exception:
            image_src = _placeholder_image_data_uri(name, category)

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
            f'<a class="market-action" href="{html.escape(product_url)}" target="_blank" rel="noopener noreferrer">🛒 Beli Sekarang</a>'
            if product_url
            else '<span class="market-action disabled">Beli Sekarang</span>'
        )
        shop_button = (
            f'<a class="market-action" href="{html.escape(shop_url)}" target="_blank" rel="noopener noreferrer">🏪 Kunjungi Toko</a>'
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
