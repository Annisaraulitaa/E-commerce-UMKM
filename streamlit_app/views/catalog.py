import html
from textwrap import dedent
from urllib.parse import quote

import pandas as pd
import streamlit as st

from components.product_card import render_product_card
from components.product_registration import render_product_registration_flow
from config import (
    INITIAL_DISPLAY,
    LOAD_MORE_STEP,
    SEARCH_POOL_SIZE,
    WEIGHT_POPULARITY,
    WEIGHT_RELEVANCE,
    WEIGHT_VALUE,
)
from utils import get_approved_submissions


FILTER_OPTIONS = ["Semua", "UMKM"]

INITIAL_RANDOM_BATCH_SIZE = INITIAL_DISPLAY
INITIAL_RANDOM_BATCH_LIMIT = 7
INITIAL_RANDOM_MAX_DISPLAY = INITIAL_RANDOM_BATCH_SIZE * INITIAL_RANDOM_BATCH_LIMIT
NEW_UMKM_DISPLAY = 15


def _html(markup):
    clean_markup = "".join(line.strip() for line in markup.splitlines())
    st.markdown(clean_markup, unsafe_allow_html=True)


def format_number(value):
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return str(value)


def get_umkm_mask(data):
    if data.empty:
        return pd.Series(False, index=data.index)

    if "umkm_label" in data.columns:
        return (
            pd.to_numeric(
                data["umkm_label"],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
            .eq(1)
        )

    if "umkm_binary" in data.columns:
        return (
            pd.to_numeric(
                data["umkm_binary"],
                errors="coerce"
            )
            .fillna(0)
            .astype(int)
            .eq(1)
        )

    return pd.Series(False, index=data.index)


def count_catalog_type(data):
    if data.empty:
        return 0, 0

    umkm_mask = get_umkm_mask(data)
    umkm_count = int(umkm_mask.sum())
    non_umkm_count = int(len(data) - umkm_count)

    return umkm_count, non_umkm_count


def apply_catalog_filter(data, filter_mode):
    if data.empty:
        return data

    umkm_mask = get_umkm_mask(data)

    if filter_mode == "UMKM":
        return data[umkm_mask]

    return data


def apply_search_sort(data, sort_mode):
    if data.empty:
        return data

    result = data.copy()

    if sort_mode == "Terbaru":

        if "shopinfo_open_since" in result.columns:
            result = result.sort_values(
                "shopinfo_open_since",
                ascending=False
            )


    elif sort_mode == "Terlaris":

        if "countSold" in result.columns:
            result = result.sort_values(
                "countSold",
                ascending=False
            )


    elif sort_mode == "Harga Terendah":

        if "price_number" in result.columns:
            result = result.sort_values(
                "price_number",
                ascending=True
            )


    elif sort_mode == "Harga Tertinggi":

        if "price_number" in result.columns:
            result = result.sort_values(
                "price_number",
                ascending=False
            )


    return result.reset_index(drop=True)


def render_search_sort():

    options = [
        "Terkait",
        "Terbaru",
        "Terlaris",
        "Harga Terendah",
        "Harga Tertinggi",
    ]


    if "sort_mode" not in st.session_state:
        st.session_state.sort_mode = "Terkait"

    with st.container(key="search_sort"):

        selected = st.radio(
            "Sort",
            options,
            index=options.index(
                st.session_state.sort_mode
            ),
            horizontal=True,
            label_visibility="collapsed",
            key="sort_radio"
        )


        st.session_state.sort_mode = selected


    return st.session_state.sort_mode


def render_search_filter_panel(data):

    filtered = data.copy()

    if "price_min_filter" not in st.session_state:
        st.session_state.price_min_filter = None

    if "price_max_filter" not in st.session_state:
        st.session_state.price_max_filter = None

    st.markdown(
        """
        <div class="filter-title">
            FILTER
        </div>
        """,
        unsafe_allow_html=True
    )


    # ==================
    # Lokasi
    # ==================

    if "shop_city" in filtered.columns:

        cities = sorted(
            filtered["shop_city"]
            .dropna()
            .unique()
            .tolist()
        )


        selected_city = st.multiselect(
            "Lokasi",
            cities
        )


        if selected_city:
            filtered = filtered[
                filtered["shop_city"].isin(selected_city)
            ]


    # ==================
    # Level Toko
    # ==================

    st.markdown(
        "<div class='filter-section-title'>Level Toko</div>",
        unsafe_allow_html=True
    )


    seller_options = [
        "Official Store",
        "Power Merchant",
        "Regular Merchant"
    ]


    selected_seller = []

    for seller in seller_options:

        checked = st.checkbox(
            seller,
            key=f"seller_{seller}"
        )

        if checked:
            selected_seller.append(seller)


    if selected_seller and "shopinfo_badge_type" in filtered.columns:

        seller_type = (
            filtered["shopinfo_badge_type"]
            .astype(str)
            .str.upper()
        )


        mask = pd.Series(
            False,
            index=filtered.index
        )


        if "Official Store" in selected_seller:
            mask |= seller_type.eq(
                "OFFICIAL_STORE"
            )


        if "Power Merchant" in selected_seller:
            mask |= seller_type.isin(
                [
                    "POWER_MERCHANT",
                    "GOLD_OR_POWER_MERCHANT"
                ]
            )


        if "Regular Merchant" in selected_seller:
            mask |= (
                ~seller_type.isin(
                    [
                        "OFFICIAL_STORE",
                        "POWER_MERCHANT",
                        "GOLD_OR_POWER_MERCHANT"
                    ]
                )
            )


        filtered = filtered[mask]



    # ==================
    # Harga
    # ==================

    st.markdown(
        "<div class='filter-section-title'>Harga</div>",
        unsafe_allow_html=True
    )


    price_col1, price_col2, price_col3 = st.columns(
        [1, 1, 0.72],
        gap="small"
    )


    with price_col1:
        min_price = st.number_input(
            "Harga Min",
            min_value=0,
            value=None,
            placeholder="Harga Min",
            label_visibility="collapsed"
        )


    with price_col2:
        max_price = st.number_input(
            "Harga Maks",
            min_value=0,
            value=None,
            placeholder="Harga Maks",
            label_visibility="collapsed"
        )
    
    with price_col3:
        if st.button(
            "PAKAI",
            use_container_width=True,
            key="apply_price_filter"
        ):
            st.session_state.price_min_filter = min_price
            st.session_state.price_max_filter = max_price
            st.rerun()


    if "price_number" in filtered.columns:

        saved_min = st.session_state.get(
            "price_min_filter",
            None
        )

        saved_max = st.session_state.get(
            "price_max_filter",
            None
        )


        if saved_min is not None and saved_min > 0:

            filtered = filtered[
                filtered["price_number"] >= saved_min
            ]


        if saved_max is not None and saved_max > 0:

            filtered = filtered[
                filtered["price_number"] <= saved_max
            ]



    # ==================
    # Rating
    # ==================

    st.markdown(
        "<div class='filter-section-title'>Rating</div>",
        unsafe_allow_html=True
    )


    rating_options = [
        "★ 5",
        "★ 4 ke atas",
        "★ 3 ke atas",
        "★ 2 ke atas"
    ]


    if "rating_filter" not in st.session_state:
        st.session_state.rating_filter = None


    for option in rating_options:

        checked = st.checkbox(
            option,
            value=False,
            key=f"rating_{option}"
        )


        if checked:
            st.session_state.rating_filter = option


    rating_filter = st.session_state.get(
        "rating_filter",
        None
    )


    if rating_filter and "ratingAverage" in filtered.columns:


        if rating_filter == "★ 5":

            filtered = filtered[
                filtered["ratingAverage"] >= 5
            ]


        elif rating_filter == "★ 4 ke atas":

            filtered = filtered[
                filtered["ratingAverage"] >= 4
            ]


        elif rating_filter == "★ 3 ke atas":

            filtered = filtered[
                filtered["ratingAverage"] >= 3
            ]


        elif rating_filter == "★ 2 ke atas":

            filtered = filtered[
                filtered["ratingAverage"] >= 2
            ]

    st.markdown(
        '</div>',
        unsafe_allow_html=True
    )

    return filtered


def get_initial_products(df, n=INITIAL_RANDOM_MAX_DISPLAY):
    cache_key = "catalog_initial_products"

    if cache_key in st.session_state:
        return st.session_state[cache_key]

    if df.empty:
        st.session_state[cache_key] = pd.DataFrame()
        return st.session_state[cache_key]

    sample_size = min(n, len(df))

    umkm_mask = get_umkm_mask(df)
    umkm_df = df[umkm_mask]
    non_umkm_df = df[~umkm_mask]

    # Mayoritas UMKM, tetapi tetap ada Non-UMKM agar filter informatif.
    umkm_target = min(int(sample_size * 0.72), len(umkm_df))
    non_umkm_target = min(sample_size - umkm_target, len(non_umkm_df))

    parts = []

    if umkm_target > 0:
        parts.append(umkm_df.sample(n=umkm_target))

    if non_umkm_target > 0:
        parts.append(non_umkm_df.sample(n=non_umkm_target))

    if parts:
        initial_products = pd.concat(parts, axis=0)
    else:
        initial_products = df.sample(n=sample_size)

    if len(initial_products) < sample_size:
        remaining = df.drop(index=initial_products.index, errors="ignore")
        need = sample_size - len(initial_products)

        if not remaining.empty and need > 0:
            initial_products = pd.concat(
                [initial_products, remaining.sample(n=min(need, len(remaining)))],
                axis=0,
            )

    initial_products = initial_products.sample(frac=1).reset_index(drop=True)

    st.session_state[cache_key] = initial_products
    return initial_products


def render_filter_pills(active_filter):
    links = []
    current_query = st.session_state.get("catalog_query", "").strip()

    for option in FILTER_OPTIONS:
        active_class = "active" if active_filter == option else ""
        label = "Semua Produk" if option == "Semua" else option

        url = f"?page=Beranda&catalog_filter={quote(option)}"

        if current_query:
            url += f"&q={quote(current_query)}"

        links.append(
            f'<a class="catalog-pill {active_class}" href="{url}" target="_self">{label}</a>'
        )

    _html(
        f"""
        <div class="catalog-filter-pills">
            {''.join(links)}
        </div>
        """
    )


def render_catalog_grid(result_view, key_prefix):
    per_row = 5
    result_view = result_view.reset_index(drop=True)

    for i in range(0, len(result_view), per_row):
        cols = st.columns(per_row, gap="small")

        for j, col in enumerate(cols):
            idx = i + j

            if idx < len(result_view):
                with col:
                    render_product_card(
                        result_view.iloc[idx],
                        rank=idx + 1,
                        key_prefix=key_prefix,
                    )


def render_register_cta():
    _html(
        """
        <div class="catalog-register-cta">
            <div>
                <div class="catalog-register-title">Punya toko yang menjual produk lokal?</div>
                <div class="catalog-register-subtitle">
                    Daftarkan produk Anda dan jangkau lebih banyak konsumen.
                </div>
            </div>

            <a class="catalog-register-button" href="?page=Beranda&register_product=1" target="_self">
                📝&nbsp; Daftar Produk Sekarang
            </a>
        </div>
        """
    )


def render_catalog_info_bar(result_view, total_result, is_search_result=False, query=""):
    umkm_count, non_umkm_count = count_catalog_type(result_view)

    fairness_html = ""
    #if is_search_result and len(result_view) > 0:
    #    fairness = round(umkm_count / len(result_view), 3)
    #    fairness_html = f'<span class="catalog-info-separator"></span>Fairness: <b>{fairness}</b>'

    if is_search_result:

        _html(
            f"""
            <div class="catalog-search-title">
                Hasil Pencarian: <b>"{query}"</b>
            </div>
            """
        )

    else:

        _html(
            f"""
            <div class="catalog-info-bar">
                <div class="catalog-info-left">
                    Menampilkan <b>{len(result_view)}</b> dari <b>{total_result}</b> produk
                    {fairness_html}
                </div>

                <div class="catalog-info-right">
                    <span class="legend-dot umkm"></span> UMKM: <b>{umkm_count}</b>
                </div>
            </div>
            """
        )


def render_empty_state():
    _html(
        """
        <div class="catalog-empty">
            <div class="catalog-empty-icon">🔎</div>
            <div class="catalog-empty-title">Produk Tidak Ditemukan</div>
            <div class="catalog-empty-desc">
                Coba gunakan kata kunci atau filter produk yang berbeda.
            </div>
        </div>
        """
    )


def render_catalog_results(result, key_prefix, show_load_more=True, is_search_result=False):
    if result.empty:
        render_empty_state()
        return

    visible_result = result.head(st.session_state.visible_count)

    render_catalog_info_bar(
        result_view=visible_result,
        total_result=len(result),
        is_search_result=is_search_result,
        query=st.session_state.get("catalog_query","")
    )

    with st.container(key=f"{key_prefix}_grid_wrap"):
        render_catalog_grid(result_view=visible_result, key_prefix=key_prefix)

    if show_load_more and st.session_state.visible_count < len(result):
        _html('<div class="load-more-spacer"></div>')

        col_left, col_btn, col_right = st.columns([3.3, 0.7, 3.3])

        with col_btn:
            if st.button("Muat Lebih Banyak", use_container_width=True):
                st.session_state.visible_count += LOAD_MORE_STEP
                st.rerun()

    elif show_load_more:
        st.write("")
        st.info("Semua produk relevan sudah ditampilkan.")


def render_new_umkm_section():

    new_products = get_approved_submissions()

    if new_products.empty:
        return


    with st.container(key="new_umkm_grid_wrap"):

        # Info bar tetap seperti sebelumnya
        _html(
            f"""
            <div class="catalog-new-umkm-bar">
                <div class="catalog-new-umkm-left">
                    <span class="catalog-new-umkm-icon">🌱</span>
                    <span>Produk UMKM Baru Bergabung</span>
                </div>

                <div class="catalog-new-umkm-right">
                    <span class="catalog-new-umkm-dot"></span>
                    Produk baru: <b>{len(new_products)}</b>
                </div>
            </div>
            """
        )

        _html('<div class="catalog-grid-mobile">')

        converted_products = []

        for _, row in new_products.iterrows():

            converted_products.append(
                {
                    "id": f"submission_{row.name}",
                    "source": "submission",
                    "name": row.get("product_name", "-"),
                    "shop_name": row.get("shop_name", "-"),
                    "price_number": row.get("estimated_price", 0),
                    "price_original": None,
                    "ratingAverage": 0,
                    "countReview": 0,
                    "countSold": 0,
                    "image_local_path": row.get("image_local_path", ""),
                    "category_breadcrumb": row.get("business_category", "UMKM"),
                    "umkm_label": 1,
                    "umkm_binary": 1,
                }
            )


        new_df = pd.DataFrame(converted_products)

        if not new_df.empty:
            render_catalog_grid(result_view=new_df.head(NEW_UMKM_DISPLAY), key_prefix="new_umkm")
        
        _html('</div>')


def load_catalog_css():
    _html(
        """
        <style>
        .stApp {
            background: #f5f5f1 !important;
        }

        header[data-testid="stHeader"] {
            height: 0px !important;
            visibility: hidden !important;
        }

        .block-container {
            max-width: 100% !important;
            padding-top: 0rem !important;
            padding-left: 0rem !important;
            padding-right: 0rem !important;
            padding-bottom: 3rem !important;
        }

        div[data-testid="stVerticalBlock"] {
            gap: 0rem !important;
        }

        :root {
            --catalog-hero-gradient: linear-gradient(
                135deg,
                #075985 0%,
                #0f67b1 45%,
                #2563eb 100%
            );
        }

        /* ===== HERO ===== */
        .st-key-catalog_hero_block {
            width: 100%;
            background: var(--catalog-hero-gradient);
            padding: 54px 24px 54px 24px;
            color: #ffffff;
        }

        .catalog-hero {
            width: 100%;
            background: var(--catalog-hero-gradient);
            background-attachment: fixed;
            padding: 56px 24px 180px 24px;
            text-align: center;
            color: #ffffff;
        }

        .catalog-hero-inner {
            max-width: 980px;
            margin: 0 auto;
            text-align: center;
        }

        .catalog-eyebrow {
            color: #b4e5ff;
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 1.8px;
            text-transform: uppercase;
            margin: 0 0 7px 0;
        }

        .catalog-title {
            color: #ffffff;
            font-size: 44px !important;
            font-weight: 900;
            line-height: 1.22;
            letter-spacing: -0.02em;
            margin: 0 auto 26px auto;
            padding-top: 0 !important;
            text-align: center;
        }

        .catalog-subtitle {
            color: #dbeafe;
            font-size: 16px;
            font-weight: 400;
            margin: 0;
            padding-top: 24px;
        }

        /* ===== SEARCH INPUT STREAMLIT ===== */
        .st-key-catalog_hero_block div[data-testid="stTextInput"] {
            max-width: 672px !important;
            margin: 26px auto 0 auto !important;
            position: relative !important;
            z-index: 20 !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] > div {
            position: relative !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] div[data-testid="InputInstructions"] {
            display: none !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] div[data-baseweb="input"] {
            width: 100% !important;
            height: 42px !important;
            min-height: 42px !important;
            border-radius: 14px !important;
            border: 1px solid rgba(255, 255, 255, 0.35) !important;
            background: #ffffff !important;
            box-shadow: 0 18px 38px rgba(15, 23, 42, 0.18) !important;
            overflow: hidden !important;
            position: relative !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] input {
            width: 100% !important;
            height: 42px !important;
            min-height: 42px !important;
            border: none !important;
            outline: none !important;
            box-shadow: none !important;

            background-color: transparent !important;
            background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='20' height='20' viewBox='0 0 24 24' fill='none' stroke='%2394a3b8' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Ccircle cx='11' cy='11' r='8'/%3E%3Cline x1='21' y1='21' x2='16.65' y2='16.65'/%3E%3C/svg%3E") !important;
            background-repeat: no-repeat !important;
            background-position: 18px center !important;
            background-size: 18px 18px !important;

            padding-left: 50px !important;
            padding-right: 18px !important;

            font-size: 14px !important;
            color: #111827 !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] input::placeholder {
            color: #94a3b8 !important;
            opacity: 1 !important;
        }

        .st-key-catalog_hero_block div[data-testid="stTextInput"] div[data-baseweb="input"]:focus-within {
            border-color: #ffffff !important;
            box-shadow:
                0 0 0 3px rgba(255, 255, 255, 0.45),
                0 18px 45px rgba(15, 23, 42, 0.18) !important;
        }

        /* ===== RESET INPUT DI MODAL / DIALOG ===== */
        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-testid="stTextInput"] {
            max-width: none !important;
            width: 100% !important;
            margin: 0 !important;
            position: static !important;
            z-index: auto !important;
        }

        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-testid="stTextInput"] > div {
            position: static !important;
        }

        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-baseweb="input"] {
            width: 100% !important;
            height: 42px !important;
            min-height: 42px !important;
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            box-shadow: none !important;
            overflow: hidden !important;
        }

        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-testid="stTextInput"] input {
            width: 100% !important;
            height: 42px !important;
            min-height: 42px !important;
            padding-left: 12px !important;
            padding-right: 12px !important;
            background-image: none !important;
            background-color: transparent !important;
            box-shadow: none !important;
            border: none !important;
            outline: none !important;
            font-size: 14px !important;
            color: #111827 !important;
        }

        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-testid="InputInstructions"] {
            display: none !important;
        }

        /* ===== RESET TEXTAREA DI MODAL ===== */
        .st-key-catalog_hero_block div[data-testid="stDialog"] textarea {
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            box-shadow: none !important;
            font-size: 14px !important;
            color: #111827 !important;
        }

        /* ===== RESET SELECTBOX DI MODAL ===== */
        .st-key-catalog_hero_block div[data-testid="stDialog"] div[data-baseweb="select"] > div {
            min-height: 42px !important;
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            box-shadow: none !important;
        }

        /* ===== RESET LABEL DI MODAL ===== */
        .st-key-catalog_hero_block div[data-testid="stDialog"] label {
            font-size: 13px !important;
            font-weight: 700 !important;
            color: #111827 !important;
            margin-bottom: 4px !important;
        }

        /* ===== PRODUCT GRID OUTER GAP ===== */
        .st-key-initial_product_grid_wrap,
        .st-key-search_result_grid_wrap {
            padding: 0 !important;
            box-sizing: border-box;
        }

        /* ===== FILTER PILLS ===== */
        .catalog-filter-pills {
            width: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 12px;
            margin-top: 22px;
            margin-bottom: 70px;
            position: relative;
            z-index: 19;
        }

        .catalog-filter-pills::before {
            display: none !important;
        }

        .catalog-pill {
            width: 150px;
            height: 36px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0 18px;
            border-radius: 999px;
            background: rgba(255,255,255,0.16);
            color: #e0f2fe !important;
            text-decoration: none !important;
            font-size: 12px;
            font-weight: 800;
            border: 1px solid rgba(255,255,255,0.28);
            transition: all 0.18s ease;
        }

        .catalog-pill:hover {
            background: rgba(255, 255, 255, 0.24);
            color: #ffffff !important;
        }

        .catalog-pill.active {
            background: #ffffff;
            color: #075985 !important;
            border-color: #ffffff;
            box-shadow: 0 12px 26px rgba(15, 23, 42, 0.18);
        }

        .catalog-register-cta {
            max-width: 672px;
            margin: 0 auto;
            padding: 18px 0 0 0;
            border-top: 1px solid rgba(255,255,255,0.18);
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 20px;
            position: relative;
            z-index: 8;
            color: #ffffff;
        }

        .catalog-register-cta::before {
            display: none !important;
        }

        .catalog-register-title {
            font-size: 14px;
            font-weight: 900;
            color: #ffffff;
            text-align: left;
        }

        .catalog-register-subtitle {
            margin-top: 0;
            font-size: 12px;
            color: rgba(255,255,255,0.72);
            text-align: left;
        }

        .catalog-register-button {
            height: 40px;
            padding: 0 20px 0 18px;
            border-radius: 13px;
            background: #ffffff;
            color: #2563eb !important;
            text-decoration: none !important;
            font-size: 15px;
            font-weight: 700;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            white-space: nowrap;
            box-shadow: 0 12px 26px rgba(15, 23, 42, 0.14);
        }

        .catalog-register-button:hover {
            background: #eff6ff;
        }

        .catalog-info-bar {
            width: 100% !important;
            max-width: none !important;
            min-height: 42px;
            margin: 5px 0 5px 0;
            padding: 0 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            color: #64748b;
            background-color: #ddeaf7;
            font-size: 13px;
            box-sizing: border-box;
        }

        /* ===== NEW UMKM SECTION BAR ===== */
        .catalog-new-umkm-bar {
            width: 100% !important;
            min-height: 42px;
            margin: 5px 0 5px 0;
            padding: 0 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            color: #64748b;
            background-color: #ddeaf7;
            font-size: 13px;
            box-sizing: border-box;
        }

        .catalog-new-umkm-left,
        .catalog-new-umkm-right {
            display: flex;
            align-items: center;
            gap: 7px;
            flex-wrap: wrap;
        }

        .catalog-new-umkm-left span:last-child {
            color: #334155;
            font-size: 13px;
            font-weight: 800;
        }

        .catalog-new-umkm-icon {
            font-size: 15px;
            line-height: 1;
        }

        .catalog-new-umkm-dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            display: inline-block;
            background: #10b981;
            margin-left: 8px;
        }

        .catalog-info-left,
        .catalog-info-right {
            display: flex;
            align-items: center;
            gap: 7px;
            flex-wrap: wrap;
        }

        .catalog-info-separator {
            width: 1px;
            height: 13px;
            background: #cbd5e1;
            display: inline-block;
            margin: 0 8px;
        }

        .catalog-search-title {
            max-width:1380px;
            margin:10px auto 15px auto;
            padding:0;
            font-size:18px;
            font-weight:700;
            color:#5d6067;
        }


        .catalog-search-title b {
            color:#0e65ab;
            font-size: 18px;
        }

        .legend-dot {
            width: 8px;
            height: 8px;
            border-radius: 999px;
            display: inline-block;
            margin-left: 8px;
        }

        .legend-dot.umkm {
            background: #10b981;
        }

        .legend-dot.non {
            background: #3b82f6;
        }

        .market-card-link {
            color: inherit !important;
            text-decoration: none !important;
            display: block;
            height: 100%;
        }

        .market-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
            transition: all 0.18s ease;
            height: 100%;
            display: flex;
            flex-direction: column;
        }

        .market-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 14px 28px rgba(15, 23, 42, 0.14);
            border-color: #cbd5e1;
        }

        .market-image-wrap {
            position: relative;
            height: 250px;
            background: #f1f5f9;
            overflow: hidden;
        }

        .market-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
        }

        .market-badge {
            position: absolute;
            top: 10px;
            left: 10px;
            height: 20px;
            padding: 0 8px;
            border-radius: 5px;
            color: #ffffff;
            font-size: 10px;
            font-weight: 900;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }

        .market-badge.umkm {
            background: #00a878;
        }

        .market-badge.non {
            background: #2b7fff;
            display: none !important;
        }

        .market-discount {
            position: absolute;
            top: 10px;
            right: 10px;
            height: 21px;
            padding: 0 8px;
            border-radius: 6px;
            background: #ff385c;
            color: #ffffff;
            font-size: 11px;
            font-weight: 950;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }

        .market-card-body {
            padding: 10px 12px 12px 12px;
            flex: 1;
            display: flex;
            flex-direction: column;
        }

        .market-category {
            height: 17px;
            font-size: 10.5px;
            color: #64748b;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
            margin-bottom: 5px;
        }

        .market-name {
            min-height: 38px;
            max-height: 38px;
            color: #0f172a;
            font-size: 13px;
            font-weight: 850;
            line-height: 1.42;
            overflow: hidden;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
        }

        .market-price-wrap {
            min-height: 38px;
            margin-top: 5px;
            margin-bottom: 4px;
        }

        .market-original-price {
            color: #94a3b8;
            font-size: 11px;
            text-decoration: line-through;
            height: 15px;
            line-height: 15px;
        }

        .market-price {
            color: #0f3764;
            font-size: 18px;
            font-weight: 950;
            line-height: 1.15;
        }

        .market-rating-row {
            display: flex;
            align-items: center;
            gap: 4px;
            color: #334155;
            font-size: 11.5px;
            margin-top: auto;
            padding-top: 14px;
        }

        .market-rating-row span:nth-child(3) {
            margin-left: 2px;
            margin-right: 2px;
        }

        .market-star {
            color: #f5b400;
            font-size: 13px;
        }

        .market-muted {
            color: #64748b;
        }

        .market-shop {
            margin-top: 4px;
            color: #64748b;
            font-size: 10.5px;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }

        .catalog-empty {
            background: #ffffff;
            border: 1px dashed #cbd5e1;
            border-radius: 18px;
            padding: 54px 24px;
            text-align: center;
            color: #64748b;
            margin-top: 24px;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.04);
        }

        .load-more-spacer {
            height: 36px;
        }

        /* Tombol Muat Lebih Banyak */
        div[data-testid="stButton"] {
            display: flex;
            justify-content: center;
        }

        div[data-testid="stButton"] button {
            min-width: 10px !important;
            height: 36px !important;
            min-height: 36px !important;
            border-radius: 10px !important;
            background: #2563eb !important;
            color: #ffffff !important;
            border: 1px solid #2563eb !important;
            font-size: 12px !important;
            font-weight: 700 !important;
            white-space: nowrap !important;
        }

        /* teks di dalam tombol */
        div[data-testid="stButton"] button p {
            font-size: 12px !important;
            font-weight: 700 !important;
            line-height: 1 !important;
            margin: 0 !important;
            padding: 0 !important;
            white-space: nowrap !important;
        }

        div[data-testid="stButton"] button:hover {
            background: #1d4ed8 !important;
            border-color: #1d4ed8 !important;
            transform: translateY(-1px);
        }

        /* ===== RESET BUTTON DI MODAL / DIALOG ===== */
        div[data-testid="stDialog"] div[data-testid="stButton"] {
            display: block !important;
            justify-content: initial !important;
        }

        div[data-testid="stDialog"] div[data-testid="stButton"] button {
            width: 100% !important;
            min-width: 0 !important;
            height: 44px !important;
            min-height: 44px !important;
            border-radius: 12px !important;
            font-size: 14px !important;
            font-weight: 800 !important;
            white-space: nowrap !important;
            transform: none !important;
        }

        .catalog-empty-icon {
            font-size: 40px;
            margin-bottom: 10px;
        }

        .catalog-empty-title {
            font-size: 20px;
            font-weight: 900;
            color: #0f172a;
            margin-bottom: 6px;
        }

        .catalog-empty-desc {
            color: #64748b;
            font-size: 14px;
        }

        /* ===== SEARCH FILTER PANEL ===== */

        /* Container filter */
        .st-key-filter_sidebar_box {
            padding-left:30px !important;
            padding-right:20px !important;
        }

        /* Judul utama FILTER */
        .st-key-filter_sidebar_box .filter-title {
            font-size:24px !important;
            font-weight:900 !important;
            color:#0f172a !important;
            margin-top:20px !important;
            margin-bottom:20px !important;
        }

        /* ===== FILTER SECTION TITLE ===== */
        .st-key-filter_sidebar_box .filter-section-title,
        .st-key-filter_sidebar_box div[data-testid="stWidgetLabel"] p {
            font-size:14px !important;
            font-weight:0 !important;
            color:#1e293b !important;
            margin-top:18px !important;
            margin-bottom:10px !important;
        }

        /* Label checkbox */
        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] label {
            font-size:15px !important;
            color:#334155 !important;
        }

        /* ===== LOCATION INPUT ===== */
        .st-key-filter_sidebar_box div[data-baseweb="select"] {
            width:150px !important;
        }

        .st-key-filter_sidebar_box div[data-baseweb="select"] > div {
            height:38px !important;
            min-height:38px !important;
            border-radius:10px !important;
            background:#ffffff !important;
            border:1px solid #cbd5e1 !important;
            box-shadow:none !important;
            color:#94a3b8 !important;
            font-size:14px !important;
        }

        /* Input harga */
        .st-key-filter_sidebar_box input {
            border-radius:10px !important;
            font-size:14px !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] {
            width:80px !important;
            margin-top: 8px !important;
        }

        /* Input harga */
        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] input {
            height:36px !important;
            border-radius:10px !important;
            font-size:12px !important;
            background:#ffffff !important;
            border:1px solid #cbd5e1 !important;
            padding-left:10px !important;
            outline:none !important;
        }

        /* Hilangkan border merah focus/error number input */
        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] div[data-baseweb="base-input"] {
            border-color:#cbd5e1 !important;
            box-shadow:none !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] div[data-baseweb="input"] {
            border-color:#cbd5e1 !important;
            box-shadow:none !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] div[data-baseweb="input"]:focus-within {
            border-color:#cbd5e1 !important;
            box-shadow:none !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] div:focus-within {
            border-color:#cbd5e1 !important;
            box-shadow:none !important;
        }

        /* Hapus tombol clear pada number input */
        .st-key-filter_sidebar_box div[data-testid="stNumberInput"] button {
            display:none !important;
            visibility:hidden !important;
            width:0 !important;
            padding:0 !important;
            margin:0 !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stHorizontalBlock"] {
            gap:16px !important;
        }

        /* placeholder */
        .st-key-filter_sidebar_box input::placeholder {
            color:#94a3b8 !important;
        }

        /* Tombol PAKAI */
        .st-key-filter_sidebar_box .st-key-apply_price_filter button {
            width:70px !important;
            height:20px !important;
            background:#2563eb !important;
            color:white !important;
            border-radius:10px !important;
            font-size:10px !important;
            font-weight:0 !important;
        }

        /* FILTER LEFT SPACING */
        .st-key-filter_sidebar_box {
            padding-left: 30px !important;
            padding-right: 15px !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] label {
            font-size:15px !important;
            color:#334155 !important;
        }

        .st-key-filter_sidebar_box input {
            border-radius:10px !important;
            font-size:14px !important;
        }

        .st-key-filter_sidebar_box div[data-baseweb="checkbox"] > div:first-child {
            border-color:#cbd5e1 !important;
        }


        .st-key-filter_sidebar_box div[data-baseweb="checkbox"] input:checked + div {
            background-color:#2563eb !important;
            border-color:#2563eb !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] input {
            accent-color:#2563eb !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] label {
            font-size:15px !important;
            color:#334155 !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] div[role="checkbox"][aria-checked="true"] {
            background-color:#2563eb !important;
            border-color:#2563eb !important;
        }

        /* ===== FILTER CHECKBOX ALIGNMENT ===== */
        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] {
            margin-bottom:4px !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] > label {
            display:flex !important;
            align-items:center !important;
            padding-top: 12px;
            gap:8px !important;
        }

        .st-key-filter_sidebar_box div[data-testid="stCheckbox"] p {
            margin:0 !important;
            line-height:1.2 !important;
        }

        /* ===== SEARCH SORT RADIO ===== */
        .st-key-search_sort {
            width:100% !important;
        }

        .st-key-search_sort [data-testid="stRadio"] > div {
            display:flex !important;
            flex-direction:row !important;
            gap:14px !important;
            align-items:center !important;
        }

        .st-key-search_sort label {
            white-space:nowrap !important;
            font-size:15px !important;
        }

        /* ===== SEARCH SORT RADIO COLOR ===== */
        .st-key-search_sort div[data-baseweb="radio"] div[role="radio"][aria-checked="true"] {
            background-color: #2563eb !important;
            border-color: #2563eb !important;
        }

        .st-key-search_sort div[data-baseweb="radio"] div[role="radio"] {
            border-color: #cbd5e1 !important;
        }

        .st-key-search_sort div[data-baseweb="radio"] div[role="radio"][aria-checked="true"]::after {
            background-color: #ffffff !important;
        }

        /* FILTER utama */
        .st-key-filter_sidebar_box .filter-title {
            font-size:20px !important;
            font-weight:800 !important;
            color:#2e4374 !important;
            margin-bottom:20px !important;
        }

        /* ===== FORCE STREAMLIT CHECKBOX BLUE ===== */
        .st-key-filter_sidebar_box [data-testid="stCheckbox"] div[role="checkbox"] {
            border-color:#2563eb !important;
        }

        .st-key-filter_sidebar_box [data-testid="stCheckbox"] div[role="checkbox"][aria-checked="true"] {
            background-color:#2563eb !important;
            border-color:#2563eb !important;
        }

        .st-key-filter_sidebar_box [data-testid="stCheckbox"] svg {
            color:white !important;
        }

        /* ===== SEARCH LOAD MORE BUTTON ===== */
        .st-key-search_load_more {
            display: flex !important;
            justify-content: center !important;
            width: 100% !important;
        }

        .st-key-search_load_more button {
            width: auto !important;
            min-width: 150px !important;
        }

        @media (max-width: 1100px) {
            .market-image-wrap {
                height: 220px;
            }
        }

        @media (max-width: 700px) {
            .catalog-hero {
                border-radius: 0 0 18px 18px;
                padding-top: 40px;
            }

            .catalog-title {
                color: #ffffff;
                font-size: 44px !important;
                font-weight: 900;
                line-height: 1.22;
                letter-spacing: -0.02em;
                margin: 0 auto;
                padding-top: 0 !important;
                text-align: center;
            }

            .catalog-support-badge {
                position: relative;
                top: auto;
                right: auto;
                width: fit-content;
                margin: 18px auto 0 auto;
            }

            .catalog-filter-pills {
                flex-wrap: wrap;
                gap: 8px;
            }

            .catalog-pill {
                min-width: auto;
                padding: 0 14px;
            }

            /* ===== RAPATKAN JARAK PRODUK SUBMITTED KE INFO BAR ===== */
            .st-key-new_umkm_grid_wrap {
                margin-bottom: 4px !important;
                padding-bottom: 0 !important;
            }

            .st-key-new_umkm_grid_wrap > div,
            .st-key-new_umkm_grid_wrap div[data-testid="stVerticalBlock"],
            .st-key-new_umkm_grid_wrap div[data-testid="stHorizontalBlock"] {
                margin-bottom: 0 !important;
                padding-bottom: 0 !important;
                row-gap: 8px !important;
            }

            .st-key-new_umkm_grid_wrap div[data-testid="column"] {
                min-height: 0 !important;
                margin-bottom: 0 !important;
                padding-bottom: 0 !important;
            }

            .st-key-new_umkm_grid_wrap .market-card-link,
            .st-key-new_umkm_grid_wrap .market-card {
                margin-bottom: 0 !important;
            }

            .catalog-info-bar {
                align-items: flex-start;
                flex-direction: column;
                margin-top: 4px !important;
            }

            .catalog-new-umkm-bar {
                margin-bottom: 4px !important;
            }

            /* ===== FILTER MOBILE ===== */
            .st-key-filter_sidebar_box {
                padding-left: 16px !important;
                padding-right: 16px !important;
            }

            .st-key-filter_sidebar_box div[data-testid="stHorizontalBlock"] {
                display: grid !important;
                grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto !important;
                gap: 8px !important;
                align-items: end !important;
            }

            .st-key-filter_sidebar_box div[data-testid="stHorizontalBlock"]
            > div[data-testid="column"] {
                width: 100% !important;
                min-width: 0 !important;
                flex: none !important;
            }

            .st-key-filter_sidebar_box div[data-testid="stNumberInput"] {
                width: 100% !important;
                margin-top: 0 !important;
            }

            .st-key-filter_sidebar_box .st-key-apply_price_filter {
                display: flex !important;
                align-items: flex-end !important;
                margin: 0 !important;
                padding: 0 !important;
            }

            .st-key-filter_sidebar_box .st-key-apply_price_filter button {
                width: 72px !important;
                min-width: 72px !important;
                height: 36px !important;
                min-height: 36px !important;
                margin: 0 !important;
                padding: 0 10px !important;
            }

            .st-key-initial_product_grid_wrap,
            .st-key-search_result_grid_wrap,
            .st-key-new_umkm_grid_wrap {
                width: 100% !important;
                max-width: 100% !important;
                padding-left: 10px !important;
                padding-right: 10px !important;
                margin-left: auto !important;
                margin-right: auto !important;
                box-sizing: border-box !important;
            }

            /* Setiap baris produk tidak lagi memakai layout 5 kolom pada mobile */
            .st-key-initial_product_grid_wrap div[data-testid="stHorizontalBlock"],
            .st-key-search_result_grid_wrap div[data-testid="stHorizontalBlock"],
            .st-key-new_umkm_grid_wrap div[data-testid="stHorizontalBlock"] {
                width: 100% !important;
                max-width: 100% !important;
                margin-left: auto !important;
                margin-right: auto !important;
                box-sizing: border-box !important;
            }

            .st-key-initial_product_grid_wrap div[data-testid="column"]:has(.market-card-link),
            .st-key-search_result_grid_wrap div[data-testid="column"]:has(.market-card-link),
            .st-key-new_umkm_grid_wrap div[data-testid="column"]:has(.market-card-link) {
                width: 100% !important;
                max-width: 100% !important;
                margin-left: auto !important;
                margin-right: auto !important;
                padding-left: 0 !important;
                padding-right: 0 !important;
                box-sizing: border-box !important;
            }

            .st-key-initial_product_grid_wrap .market-card-link,
            .st-key-search_result_grid_wrap .market-card-link,
            .st-key-new_umkm_grid_wrap .market-card-link,
            .st-key-initial_product_grid_wrap .market-card,
            .st-key-search_result_grid_wrap .market-card,
            .st-key-new_umkm_grid_wrap .market-card {
                width: 100% !important;
                max-width: 100% !important;
                margin-left: auto !important;
                margin-right: auto !important;
                box-sizing: border-box !important;
            }

            /* Sembunyikan empat kolom kosong hasil dari st.columns(5) */
            .st-key-initial_product_grid_wrap div[data-testid="column"]:not(:has(.market-card-link)),
            .st-key-search_result_grid_wrap div[data-testid="column"]:not(:has(.market-card-link)),
            .st-key-new_umkm_grid_wrap div[data-testid="column"]:not(:has(.market-card-link)) {
                display: none !important;
            }

            /* Jarak yang konsisten antar-card */
            .st-key-initial_product_grid_wrap .market-card-link,
            .st-key-search_result_grid_wrap .market-card-link,
            .st-key-new_umkm_grid_wrap .market-card-link {
                display: block !important;
                width: 100% !important;
                margin: 0 0 12px 0 !important;
                padding: 0 !important;
            }

            /* Produk terakhir tidak membutuhkan jarak terlalu besar */
            .st-key-initial_product_grid_wrap
            div[data-testid="stHorizontalBlock"]:last-child
            .market-card-link,

            .st-key-search_result_grid_wrap
            div[data-testid="stHorizontalBlock"]:last-child
            .market-card-link,

            .st-key-new_umkm_grid_wrap
            div[data-testid="stHorizontalBlock"]:last-child
            .market-card-link {
                margin-bottom: 4px !important;
            }

            /* Hilangkan jarak bawaan wrapper Streamlit */
            .st-key-initial_product_grid_wrap div[data-testid="stVerticalBlock"],
            .st-key-search_result_grid_wrap div[data-testid="stVerticalBlock"],
            .st-key-new_umkm_grid_wrap div[data-testid="stVerticalBlock"] {
                gap: 0 !important;
            }

            .st-key-initial_product_grid_wrap div[data-testid="stElementContainer"],
            .st-key-search_result_grid_wrap div[data-testid="stElementContainer"],
            .st-key-new_umkm_grid_wrap div[data-testid="stElementContainer"] {
                margin: 0 !important;
                padding: 0 !important;
            }

            /* ===== GAMBAR PRODUCT CARD MOBILE ===== */
            .market-image-wrap {
                width: 100% !important;
                height: 300px !important;
                overflow: hidden !important;
                background: #f1f5f9 !important;
            }

            .market-image {
                width: 100% !important;
                height: 100% !important;
                object-fit: cover !important;
                object-position: center !important;
                transform: scale(1.06);
            }

        }
        </style>
        """
    )


def render_catalog_page(df, recommender):

    if st.query_params.get("product_id"):
        return

    if "result" not in st.session_state:
        st.session_state.result = pd.DataFrame()

    if "last_query" not in st.session_state:
        st.session_state.last_query = ""

    if "visible_count" not in st.session_state:
        st.session_state.visible_count = INITIAL_DISPLAY

    if "catalog_filter" not in st.session_state:
        st.session_state.catalog_filter = "Semua"

    if "catalog_view_mode" not in st.session_state:
        st.session_state.catalog_view_mode = "initial"
    
    query_from_url = st.query_params.get("q", "")

    if query_from_url and st.session_state.get("catalog_query", "") == "":
        st.session_state.catalog_query = query_from_url

    filter_from_url = st.query_params.get("catalog_filter", st.session_state.catalog_filter)
    if filter_from_url in FILTER_OPTIONS:
        st.session_state.catalog_filter = filter_from_url

    filter_mode = st.session_state.catalog_filter

    total_products = len(df)
    total_umkm, total_non_umkm = count_catalog_type(df)

    load_catalog_css()

    hero_html = (
        '<div class="catalog-hero-inner">'
        '<p class="catalog-eyebrow">Ayo Dukung Produk Lokal</p>'
        '<h1 class="catalog-title">Temukan Produk UMKM Terbaik<br>Untuk Anda</h1>'
        f'<p class="catalog-subtitle">{total_umkm:,} produk UMKM &nbsp;·&nbsp; {total_non_umkm:,} produk Non-UMKM &nbsp;·&nbsp; {total_products:,} total produk</p>'
        '</div>'
    ).replace(",", ".")

    with st.container(key="catalog_hero_block"):
        st.markdown(hero_html, unsafe_allow_html=True)

        query = st.text_input(
            "Cari produk",
            placeholder="Cari produk...",
            label_visibility="collapsed",
            key="catalog_query"
        )

        query_clean = query.strip()

        url_query = st.query_params.get("q", "").strip()
        url_filter = st.query_params.get("catalog_filter", "")

        if query_clean:
            if url_query == query_clean and url_filter in FILTER_OPTIONS:
                display_filter_mode = url_filter
            else:
                display_filter_mode = "UMKM"
        else:
            display_filter_mode = filter_mode

        render_filter_pills(active_filter=display_filter_mode)

        render_register_cta()


    if query_clean:
        if st.session_state.catalog_view_mode != "search":
            st.session_state.visible_count = INITIAL_DISPLAY
            st.session_state.catalog_view_mode = "search"
            
        if st.session_state.last_query != query_clean:
            result = recommender.search(
                query=query_clean,
                top_n=SEARCH_POOL_SIZE,
                weight_relevance=WEIGHT_RELEVANCE,
                weight_popularity=WEIGHT_POPULARITY,
                weight_value=WEIGHT_VALUE,
            )

            st.session_state.result = result
            st.session_state.last_query = query_clean
            st.session_state.visible_count = INITIAL_DISPLAY

        url_query = st.query_params.get("q", "").strip()
        url_filter = st.query_params.get("catalog_filter", "")

        if url_query == query_clean and url_filter in FILTER_OPTIONS:
            search_filter_mode = url_filter
        else:
            search_filter_mode = "UMKM"

        filtered_result = apply_catalog_filter(
            st.session_state.result,
            search_filter_mode
        )


        # =========================
        # SEARCH RESULT LAYOUT
        # =========================

        left_filter, right_result = st.columns(
            [0.9, 5],
            gap="medium"
        )


        with left_filter:

            with st.container(key="filter_sidebar_box"):

                filtered_result = render_search_filter_panel(
                    filtered_result
                )


        with right_result:

            sort_mode = render_search_sort()


            filtered_result = apply_search_sort(
                filtered_result,
                sort_mode
            )


            visible_result = filtered_result.head(
                st.session_state.visible_count
            )

            render_catalog_info_bar(
                result_view=visible_result,
                total_result=len(filtered_result),
                is_search_result=True,
                query=query_clean
            )

            with st.container(key="search_result_grid_wrap"):
                render_catalog_grid(
                    result_view=visible_result,
                    key_prefix="search_result"
                )


            if st.session_state.visible_count < len(filtered_result):

                _html('<div class="load-more-spacer"></div>')

                if st.button(
                    "Muat Lebih Banyak",
                    use_container_width=False,
                    key="search_load_more"
                ):
                    st.session_state.visible_count += LOAD_MORE_STEP
                    st.rerun()

    else:
        if st.session_state.catalog_view_mode != "initial":
            st.session_state.visible_count = INITIAL_DISPLAY
            st.session_state.catalog_view_mode = "initial"

        st.session_state.result = pd.DataFrame()
        st.session_state.last_query = ""

        initial_products = get_initial_products(df, n=INITIAL_RANDOM_MAX_DISPLAY)
        filtered_initial = apply_catalog_filter(initial_products, filter_mode)

        render_new_umkm_section()

        render_catalog_results(
            result=filtered_initial,
            key_prefix="initial_product",
            show_load_more=True,
            is_search_result=False,
        )

        render_product_registration_flow()