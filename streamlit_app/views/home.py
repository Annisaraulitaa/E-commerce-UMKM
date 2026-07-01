from urllib.parse import quote

import pandas as pd
import streamlit as st

from components.metric_card import render_metric_mini


def render_home_page(df):
    total_products = len(df)
    total_umkm = int(df["umkm_binary"].sum()) if "umkm_binary" in df.columns else 0
    total_non_umkm = total_products - total_umkm

    catalog_url = f"?page={quote('Katalog Produk')}"

    hero_html = (
        '<div class="hero-section">'
        '<div class="hero-title">Temukan Produk UMKM Terbaik<br>Untuk Anda</div>'
        '<div class="hero-desc">'
        'Cari produk UMKM dan non-UMKM dalam satu tempat'
        '</div>'
        '<div class="hero-desc">'
        'Produk UMKM kami tampilkan lebih dulu agar usaha kecil lebih mudah ditemukan'
        '</div>'
        '<div class="hero-desc">'
        '- Cepat, Relevan, dan Adil bagi pelaku usaha kecil'
        '</div>'
        '<div class="hero-actions">'
        f'<a class="hero-primary-btn" href="{catalog_url}" target="_self">Lihat Katalog →</a>'
        '</div>'
        '</div>'
    )

    st.markdown(hero_html, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        render_metric_mini("Total Produk", f"{total_products:,}".replace(",", "."))

    with col2:
        render_metric_mini("UMKM", f"{total_umkm:,}".replace(",", "."))

    with col3:
        render_metric_mini("Non-UMKM", f"{total_non_umkm:,}".replace(",", "."))

    st.markdown('<div class="section-heading">Kategori Produk</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-desc">Sistem membedakan produk berdasarkan label UMKM dan Non-UMKM.</div>',
        unsafe_allow_html=True
    )

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            """
            <div class="green-card">
                <h3 style="color:#166534;margin-bottom:10px;">Produk UMKM</h3>
                <p style="color:#334155;line-height:1.7;">
                    Produk dari pelaku usaha mikro, kecil, dan menengah yang diprioritaskan
                    pada batch awal hasil rekomendasi untuk meningkatkan visibilitasnya.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c2:
        st.markdown(
            """
            <div class="blue-card">
                <h3 style="color:#1d4ed8;margin-bottom:10px;">Produk Non-UMKM</h3>
                <p style="color:#334155;line-height:1.7;">
                    Produk dari toko atau brand yang tidak termasuk kategori UMKM,
                    namun tetap dapat muncul apabila relevan terhadap query pengguna.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )