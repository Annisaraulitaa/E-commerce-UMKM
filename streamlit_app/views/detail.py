import html

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
    total_rating = safe_int(get_value(product, "totalRating", default=0))
    review = safe_int(get_value(product, "countReview", default=0))

    shop_name = html.escape(str(get_value(product, "shop_name", default="-")))
    shop_url = _clean_url(get_value(product, "shop_url", default=""))
    shop_city = html.escape(str(get_value(product, "shop_city", default="-")))
    shop_tier = get_value(product, "shop_tier", default="-")

    label_titles = html.escape(str(get_value(product, "label_titles", default="-")))
    umkm_label = str(get_value(product, "umkm_label", default="-")).upper()

    badge_color = "#16a34a" if umkm_label == "UMKM" else "#2563eb"

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
        .block-container {
            max-width: 1280px;
            padding-top: 1rem;
            padding-left: 2rem;
            padding-right: 2rem;
            padding-bottom: 2rem;
        }

        body {
            background: #0f172a;
        }

        .detail-shell {
            background: #ffffff;
            border-radius: 18px;
            box-shadow: 0 22px 60px rgba(15, 23, 42, 0.22);
            overflow: hidden;
            border: 1px solid #e5e7eb;
        }

        .detail-topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 20px;
            border-bottom: 1px solid #e5e7eb;
            background: #ffffff;
        }

        .detail-icons {
            display: flex;
            gap: 16px;
            color: #64748b;
            font-size: 20px;
        }

        .detail-category {
            padding: 18px 20px;
            color: #475569;
            font-size: 14px;
            border-bottom: 1px solid #e5e7eb;
            background: #ffffff;
        }

        .detail-main {
            padding: 26px 20px 28px 20px;
        }

        .detail-image-box {
            height: 520px;
            background: #f8fafc;
            border-radius: 16px;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 1px solid #e5e7eb;
        }

        .detail-image-box img {
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
            background: #ffffff;
        }

        .detail-empty-image {
            height: 520px;
            background: #f1f5f9;
            border-radius: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #94a3b8;
            font-weight: 700;
            border: 1px solid #e5e7eb;
        }

        .detail-badge-row {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
            margin-bottom: 18px;
        }

        .detail-title {
            font-size: 34px;
            line-height: 1.18;
            color: #0f172a;
            font-weight: 900;
            margin-bottom: 18px;
        }

        .detail-meta {
            font-size: 16px;
            color: #475569;
            margin-bottom: 22px;
        }

        .detail-price-box {
            background: #f8fafc;
            border-radius: 16px;
            padding: 22px 24px;
            margin-bottom: 22px;
        }

        .detail-original-price {
            font-size: 16px;
            color: #94a3b8;
            text-decoration: line-through;
            margin-bottom: 4px;
        }

        .detail-price {
            font-size: 38px;
            font-weight: 900;
            color: #2563eb;
            margin-bottom: 4px;
        }

        .detail-saving {
            font-size: 14px;
            color: #16a34a;
            font-weight: 600;
        }

        .shop-box {
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 18px;
            margin-bottom: 22px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 14px;
        }

        .shop-name {
            font-size: 18px;
            font-weight: 850;
            color: #0f172a;
            margin-bottom: 4px;
        }

        .shop-location {
            color: #64748b;
            font-size: 14px;
        }

        .benefit-list {
            margin-top: 16px;
            color: #475569;
            font-size: 14px;
            line-height: 1.8;
        }

        .description-section {
            padding: 26px 20px 32px 20px;
            border-top: 1px solid #e5e7eb;
        }

        .description-title {
            font-size: 24px;
            font-weight: 900;
            color: #0f172a;
            margin-bottom: 16px;
        }

        .description-section p {
            color: #334155;
            line-height: 1.8;
            font-size: 15px;
            margin-bottom: 14px;
        }

        div[data-testid="stImage"] {
            height: auto !important;
            overflow: visible !important;
            background: transparent !important;
        }

        div[data-testid="stImage"] img {
            height: auto !important;
            max-height: none !important;
            object-fit: contain !important;
            border-radius: 16px !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<div class="detail-shell">', unsafe_allow_html=True)

    if st.button("← Kembali", use_container_width=False):
        st.session_state.current_page = "Katalog Produk"
        st.query_params["page"] = "Katalog Produk"
        st.rerun()

    _html([
        '<div class="detail-category">',
        category,
        '</div>'
    ])

    st.markdown('<div class="detail-main">', unsafe_allow_html=True)

    col_img, col_info = st.columns([0.9, 1.15], gap="large")

    with col_img:
        if image_path is not None:
            image_src = str(image_path).replace("\\", "/")
            st.image(str(image_path), use_container_width=True)

            # fallback kalau browser tidak mengizinkan file://
            if False:
                st.image(str(image_path), use_container_width=True)
        else:
            st.markdown(
                """
                <div class="detail-empty-image">
                    Gambar tidak tersedia
                </div>
                """,
                unsafe_allow_html=True
            )

    with col_info:
        badge_parts = [
            '<div class="detail-badge-row">',
            f'<span style="background:{badge_color};color:white;padding:8px 16px;border-radius:999px;font-size:13px;font-weight:850;">{umkm_label}</span>'
        ]

        if label_titles not in ["", "-"]:
            badge_parts.append(
                f'<span style="background:#FEF3C7;color:#92400E;padding:8px 16px;border-radius:999px;font-size:13px;font-weight:750;">{label_titles}</span>'
            )

        badge_parts.append("</div>")
        _html(badge_parts)

        _html([
            '<div class="detail-title">',
            name,
            '</div>',
            '<div class="detail-meta">',
            f'⭐ {rating} &nbsp; | &nbsp; {review} ulasan &nbsp; | &nbsp; {sold} terjual',
            '</div>'
        ])

        price_parts = ['<div class="detail-price-box">']

        if original_price != "Rp 0" and original_price != price:
            price_parts.append(f'<div class="detail-original-price">{original_price}</div>')

        price_parts.extend([
            f'<div class="detail-price">{price}</div>',
            f'<div class="detail-saving">Hemat {saving_text}</div>' if saving > 0 else f'<div class="detail-saving">Diskon {discount}%</div>',
            '</div>'
        ])

        _html(price_parts)

        _html([
            '<div class="shop-box">',
            '<div>',
            f'<div class="shop-name">{shop_name}</div>',
            f'<div class="shop-location">📍 {shop_city} <span style="color:#94a3b8;">(Tier toko: {shop_tier})</span></div>',
            '</div>',
            '</div>'
        ])

        col_buy, col_shop = st.columns(2)

        with col_buy:
            if product_url:
                st.link_button("Beli Sekarang", product_url, use_container_width=True)
            else:
                st.button("Beli Sekarang", disabled=True, use_container_width=True)

        with col_shop:
            if shop_url:
                st.link_button("Kunjungi Toko", shop_url, use_container_width=True)
            else:
                st.button("Kunjungi Toko", disabled=True, use_container_width=True)

    st.markdown('</div>', unsafe_allow_html=True)

    description_html = (
        f'<p>'
        f'{name} merupakan produk pada kategori <b>{category}</b>. '
        f'Produk ini termasuk dalam kelompok <b>{umkm_label}</b> '
        f'dan memiliki diskon sebesar <b>{discount}%</b>.'
        f'</p>'
    )

    performance_html = (
        f'<p>'
        f'Berdasarkan data produk, item ini memiliki rating <b>{rating}</b> '
        f'dari total <b>{total_rating}</b> penilaian, dengan <b>{review}</b> ulasan, '
        f'dan telah terjual sebanyak <b>{sold}</b> unit.'
    )

    if original_price != "Rp 0" and original_price != price:
        performance_html += f' Harga awal produk ini adalah <b>{original_price}</b>.'

    performance_html += '</p>'

    label_info_html = (
        f'<p><b>{label_titles}</b></p>'
        if label_titles not in ["", "-"]
        else ""
    )

    _html([
        '<div class="description-section">',
        '<div class="description-title">Deskripsi Produk</div>',
        description_html,
        performance_html,
        label_info_html,
        '</div>'
    ])

    st.markdown('</div>', unsafe_allow_html=True)