import html
from textwrap import dedent
from urllib.parse import quote

import pandas as pd
import streamlit as st

from components.product_card import render_product_card
from config import (
    INITIAL_DISPLAY,
    LOAD_MORE_STEP,
    WEIGHT_POPULARITY,
    WEIGHT_RELEVANCE,
    WEIGHT_UMKM,
    WEIGHT_VALUE,
)


FILTER_OPTIONS = ["Semua", "UMKM"]


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
        return pd.Series(dtype=bool)

    if "umkm_binary" in data.columns:
        return (
            pd.to_numeric(data["umkm_binary"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )

    if "umkm_label" in data.columns:
        return data["umkm_label"].astype(str).str.upper().eq("UMKM")

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


def get_initial_products(df, n=INITIAL_DISPLAY):
    cache_key = "catalog_initial_products"

    if cache_key in st.session_state:
        return st.session_state[cache_key]

    if df.empty:
        st.session_state[cache_key] = pd.DataFrame()
        return st.session_state[cache_key]

    umkm_mask = get_umkm_mask(df)
    umkm_df = df[umkm_mask]
    non_umkm_df = df[~umkm_mask]

    # Seperti desain Figma: mayoritas UMKM, tetapi tetap ada Non-UMKM agar filter informatif.
    umkm_target = min(int(n * 0.72), len(umkm_df))
    non_umkm_target = min(n - umkm_target, len(non_umkm_df))

    parts = []

    if umkm_target > 0:
        parts.append(umkm_df.sample(n=umkm_target))

    if non_umkm_target > 0:
        parts.append(non_umkm_df.sample(n=non_umkm_target))

    if parts:
        initial_products = pd.concat(parts, axis=0)
    else:
        initial_products = df.sample(n=min(n, len(df)))

    if len(initial_products) < min(n, len(df)):
        remaining = df.drop(index=initial_products.index, errors="ignore")
        need = min(n, len(df)) - len(initial_products)

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

    for option in FILTER_OPTIONS:
        active_class = "active" if active_filter == option else ""
        label = "Semua Produk" if option == "Semua" else option
        url = f"?page=Beranda&catalog_filter={quote(option)}"

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


def build_filter_pills_html(active_filter, current_query=""):
    links = []

    for option in FILTER_OPTIONS:
        active_class = "active" if active_filter == option else ""
        label = "Semua Produk" if option == "Semua" else option

        url = (
            f"?page=Beranda"
            f"&catalog_filter={quote(option)}"
        )

        if current_query:
            url += f"&q={quote(current_query)}"

        links.append(
            f'<a class="catalog-pill {active_class}" href="{url}" target="_self">{label}</a>'
        )

    return "".join(links)


def render_catalog_grid(result_view, key_prefix):
    per_row = 5
    result_view = result_view.reset_index(drop=True)

    for i in range(0, len(result_view), per_row):
        cols = st.columns(per_row, gap="medium")

        for j, col in enumerate(cols):
            idx = i + j

            if idx < len(result_view):
                with col:
                    render_product_card(
                        result_view.iloc[idx],
                        rank=idx + 1,
                        key_prefix=key_prefix,
                    )


def render_catalog_info_bar(result_view, total_result, is_search_result=False):
    umkm_count, non_umkm_count = count_catalog_type(result_view)

    fairness_html = ""
    if is_search_result and len(result_view) > 0:
        fairness = round(umkm_count / len(result_view), 3)
        fairness_html = f'<span class="catalog-info-separator"></span>Fairness: <b>{fairness}</b>'

    _html(
        f"""
        <div class="catalog-info-bar">
            <div class="catalog-info-left">
                Menampilkan <b>{len(result_view)}</b> dari <b>{total_result}</b> produk
                {fairness_html}
            </div>

            <div class="catalog-info-right">
                <span class="legend-dot umkm"></span> UMKM: <b>{umkm_count}</b>
                <span class="legend-dot non"></span> Non-UMKM: <b>{non_umkm_count}</b>
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
    )

    render_catalog_grid(result_view=visible_result, key_prefix=key_prefix)

    if show_load_more and st.session_state.visible_count < len(result):
        st.write("")
        col_left, col_btn, col_right = st.columns([2.2, 1, 2.2])

        with col_btn:
            if st.button("Muat Lebih Banyak", use_container_width=True):
                st.session_state.visible_count += LOAD_MORE_STEP
                st.rerun()

    elif show_load_more:
        st.write("")
        st.info("Semua produk relevan sudah ditampilkan.")


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

        /* ===== HERO ===== */
        .catalog-hero {
            width: 100%;
            background: #0054a3;
            padding: 56px 24px 120px 24px;
            text-align: center;
            color: #ffffff;
        }

        .catalog-hero-inner {
            max-width: 760px;
            margin: 0 auto;
            text-align: center;
        }

        .catalog-eyebrow {
            color: rgba(255,255,255,0.62);
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 1.8px;
            text-transform: uppercase;
            margin: 0 0 18px 0;
        }

        .catalog-title {
            color: #ffffff;
            font-size: 44px !important;
            font-weight: 900;
            line-height: 1.22;
            letter-spacing: -0.02em;
            margin: 0 0 26px 0;
        }

        .catalog-subtitle {
            color: rgba(255,255,255,0.72);
            font-size: 15px;
            font-weight: 600;
            margin: 0;
        }

        /* ===== SEARCH INPUT STREAMLIT ===== */
        div[data-testid="stTextInput"] {
            max-width: 672px !important;
            margin: -118px auto 0 auto !important;
            position: relative !important;
            z-index: 20 !important;
        }

        div[data-testid="stTextInput"] > div {
            position: relative !important;
        }

        div[data-testid="stTextInput"] div[data-testid="InputInstructions"] {
            display: none !important;
        }

        div[data-testid="stTextInput"] div[data-baseweb="input"] {
            width: 100% !important;
            height: 52px !important;
            margin-top: 10px !important;
            min-height: 52px !important;
            border-radius: 14px !important;
            border: 1px solid #e5e7eb !important;
            background: #ffffff !important;
            box-shadow: 0 18px 38px rgba(15, 23, 42, 0.22) !important;
            overflow: hidden !important;
            position: relative !important;
        }

        div[data-testid="stTextInput"] input {
            width: 100% !important;
            height: 52px !important;
            min-height: 52px !important;
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

        div[data-testid="stTextInput"] input::placeholder {
            color: #9ca3af !important;
            opacity: 1 !important;
        }

        div[data-testid="stTextInput"] div[data-baseweb="input"]:focus-within {
            border-color: #ffffff !important;
            box-shadow:
                0 0 0 3px rgba(255, 255, 255, 0.45),
                0 18px 38px rgba(15, 23, 42, 0.22) !important;
        }

        /* ===== FILTER PILLS ===== */
        .catalog-filter-pills {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 12px;
            margin-top: 18px;
            margin-bottom: 48px;
            position: relative;
            z-index: 19;
            isolation: isolate;
        }

        .catalog-filter-pills::before {
            content: "";
            position: absolute;
            left: 50%;
            top: -92px;
            transform: translateX(-50%);
            width: 100vw;
            height: 136px;
            background: #0054a3;
            z-index: -1;
            pointer-events: none;
        }

        .catalog-pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            height: 31px;
            padding: 0 28px;
            border-radius: 999px;
            background: rgba(255,255,255,0.16);
            color: #ffffff !important;
            text-decoration: none !important;
            font-size: 12px;
            font-weight: 800;
            border: 1px solid rgba(255,255,255,0.12);
            transition: all 0.18s ease;
        }

        .catalog-pill:hover {
            background: rgba(255,255,255,0.25);
        }

        .catalog-pill.active {
            background: #ffffff;
            color: #173b5f !important;
            border-color: #ffffff;
            box-shadow: 0 12px 26px rgba(15, 23, 42, 0.18);
        }

        .catalog-info-bar {
            max-width: 1380px;
            min-height: 42px;
            margin: 20px auto 1px auto;
            padding: 0 8px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            color: #64748b;
            font-size: 13px;
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
            border-radius: 13px;
            overflow: hidden;
            box-shadow: 0 5px 14px rgba(15, 23, 42, 0.08);
            transition: all 0.18s ease;
            height: 100%;
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
            padding: 11px 13px 12px 13px;
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
            gap: 5px;
            color: #334155;
            font-size: 11.5px;
            margin-top: 6px;
        }

        .market-star {
            color: #f5b400;
            font-size: 13px;
        }

        .market-muted {
            color: #64748b;
        }

        .market-shop {
            margin-top: 8px;
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
                font-size: 26px !important;
                font-weight: 900;
                line-height: 1.22;
                letter-spacing: -0.02em;
                margin: 0 0 7px 0;
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

            .catalog-info-bar {
                align-items: flex-start;
                flex-direction: column;
            }
        }
        </style>
        """
    )


def render_catalog_page(df, recommender):
    if "result" not in st.session_state:
        st.session_state.result = pd.DataFrame()

    if "last_query" not in st.session_state:
        st.session_state.last_query = ""

    if "visible_count" not in st.session_state:
        st.session_state.visible_count = INITIAL_DISPLAY

    if "catalog_filter" not in st.session_state:
        st.session_state.catalog_filter = "Semua"
    
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
        '<div class="catalog-hero">'
        '<div class="catalog-hero-inner">'
        '<p class="catalog-eyebrow">Ayo Dukung Produk Lokal</p>'
        '<h1 class="catalog-title">Temukan Produk UMKM Terbaik Untuk Anda</h1>'
        f'<p class="catalog-subtitle">{total_umkm:,} produk UMKM &nbsp;·&nbsp; {total_non_umkm:,} produk Non-UMKM &nbsp;·&nbsp; {total_products:,} total produk</p>'
        '</div>'
        '</div>'
    ).replace(",", ".")

    st.markdown(hero_html, unsafe_allow_html=True)

    query = st.text_input(
        "Cari produk",
        placeholder="Cari produk, nama toko, kota, atau kategori...",
        label_visibility="collapsed",
        key="catalog_query"
    )

    render_filter_pills(filter_mode)

    query_clean = query.strip()

    if query_clean:
        if st.session_state.last_query != query_clean:
            result = recommender.search(
                query=query_clean,
                top_n=len(df),
                weight_relevance=WEIGHT_RELEVANCE,
                weight_popularity=WEIGHT_POPULARITY,
                weight_value=WEIGHT_VALUE,
                weight_umkm=WEIGHT_UMKM,
                first_umkm_quota=INITIAL_DISPLAY,
            )

            st.session_state.result = result
            st.session_state.last_query = query_clean
            st.session_state.visible_count = INITIAL_DISPLAY

        filtered_result = apply_catalog_filter(st.session_state.result, filter_mode)

        render_catalog_results(
            result=filtered_result,
            key_prefix="search_result",
            show_load_more=True,
            is_search_result=True,
        )

    else:
        st.session_state.result = pd.DataFrame()
        st.session_state.last_query = ""
        st.session_state.visible_count = INITIAL_DISPLAY

        initial_products = get_initial_products(df, n=INITIAL_DISPLAY)
        filtered_initial = apply_catalog_filter(initial_products, filter_mode)

        render_catalog_results(
            result=filtered_initial,
            key_prefix="initial_product",
            show_load_more=False,
            is_search_result=False,
        )
