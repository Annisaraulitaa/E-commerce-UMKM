import pandas as pd
import streamlit as st

from components.metric_card import render_metric_mini
from components.product_card import render_product_card
from config import (
    INITIAL_DISPLAY,
    LOAD_MORE_STEP,
    WEIGHT_POPULARITY,
    WEIGHT_RELEVANCE,
    WEIGHT_UMKM,
    WEIGHT_VALUE,
)
from metrics import count_umkm_at_k, fairness_at_k


def render_catalog_page(df, recommender):
    with st.container(border=True):
        st.markdown(
            """
            <div style="
                display:flex;
                justify-content:space-between;
                align-items:center;
                margin-bottom:14px;
            ">
                <div>
                    <div style="font-size:18px;font-weight:850;color:#0f172a;">
                        Cari Produk
                    </div>
                    <div style="font-size:14px;color:#64748b;">
                        Masukkan kata kunci produk untuk mendapatkan rekomendasi.
                    </div>
                </div>
                <div style="
                    background:#eff6ff;
                    color:#2563eb;
                    padding:8px 12px;
                    border-radius:999px;
                    font-size:13px;
                    font-weight:800;
                ">
                    UMKM-Aware Ranking
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        col_search, col_button = st.columns([5, 1.15], gap="medium", vertical_alignment="bottom")

        with col_search:
            query = st.text_input(
                "Cari produk",
                placeholder="Contoh: baju batik wanita, kopi khas daerah, kerajinan anyaman",
                label_visibility="collapsed"
            )

        with col_button:
            search_clicked = st.button("Cari", use_container_width=True)

    if search_clicked:
        if query.strip() == "":
            st.warning("Masukkan query terlebih dahulu.")
        else:
            result = recommender.search(
                query=query,
                top_n=len(df),
                weight_relevance=WEIGHT_RELEVANCE,
                weight_popularity=WEIGHT_POPULARITY,
                weight_value=WEIGHT_VALUE,
                weight_umkm=WEIGHT_UMKM,
                first_umkm_quota=INITIAL_DISPLAY
            )

            st.session_state.result = result
            st.session_state.last_query = query
            st.session_state.visible_count = INITIAL_DISPLAY

    if not st.session_state.result.empty:
        render_catalog_results()
    else:
        st.markdown(
            """
            <div style="
                background:#ffffff;
                border:1px dashed #cbd5e1;
                border-radius:18px;
                padding:54px 24px;
                text-align:center;
                color:#64748b;
                margin-top:24px;
                box-shadow:0 8px 22px rgba(15,23,42,0.04);
            ">
                <div style="font-size:42px;margin-bottom:10px;">🔎</div>
                <div style="font-size:20px;font-weight:800;color:#0f172a;margin-bottom:6px;">
                    Mulai Cari Produk
                </div>
                <div>
                    Hasil rekomendasi akan muncul setelah kamu memasukkan kata kunci pencarian.
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


def render_catalog_results():
    result = st.session_state.result
    visible_result = result.head(st.session_state.visible_count)

    umkm_flags = visible_result["umkm_binary"].astype(int).to_numpy()
    fairness = fairness_at_k(umkm_flags, k=len(visible_result))
    umkm_count = count_umkm_at_k(umkm_flags, k=len(visible_result))
    non_umkm_count = len(visible_result) - umkm_count

    st.markdown(
        f"""
        <div class="result-panel">
            <div class="result-title">
                Hasil rekomendasi untuk "{st.session_state.last_query}"
            </div>
            <div class="result-subtitle">
                Menampilkan {len(visible_result)} dari {len(result)} produk relevan.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    m1, m2, m3, m4 = st.columns(4)

    with m1:
        render_metric_mini("Ditampilkan", len(visible_result))

    with m2:
        render_metric_mini("Fairness", round(fairness, 3))

    with m3:
        render_metric_mini("Produk UMKM", umkm_count)

    with m4:
        render_metric_mini("Non-UMKM", non_umkm_count)

    st.markdown(
        """
        <div class="sort-box">
            <div>Urutkan: <b>Final Score Tertinggi</b></div>
            <div>Batch tampilan: <b>60 produk</b></div>
        </div>
        """,
        unsafe_allow_html=True
    )

    per_row = 5
    result_view = visible_result.reset_index(drop=True)

    for i in range(0, len(result_view), per_row):
        cols = st.columns(per_row, gap="medium")

        for j, col in enumerate(cols):
            idx = i + j

            if idx < len(result_view):
                with col:
                    render_product_card(result_view.iloc[idx], rank=idx + 1)

    st.write("")

    if st.session_state.visible_count < len(result):
        col_left, col_btn, col_right = st.columns([2, 1, 2])

        with col_btn:
            if st.button("Muat Lebih Banyak", use_container_width=True):
                st.session_state.visible_count += LOAD_MORE_STEP
                st.rerun()
    else:
        st.info("Semua produk relevan sudah ditampilkan.")