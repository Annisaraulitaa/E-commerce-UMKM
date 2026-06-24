from urllib.parse import quote

import streamlit as st


def render_navbar():
    current = st.session_state.current_page

    def nav_class(page_name):
        return "nav-link active" if current == page_name else "nav-link"

    st.markdown('<div class="nav-card">', unsafe_allow_html=True)

    col_brand, col_menu = st.columns([2.4, 3.6])

    with col_brand:
        st.markdown(
            """
            <div class="brand-area">
                <div class="brand-icon">🛍️</div>
                <div>
                    <div class="brand-title">Katalog UMKM</div>
                    <div class="brand-subtitle">Sistem Informasi Produk</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with col_menu:
        st.markdown(
            f'''
<div class="nav-menu">
    <a class="{nav_class("Beranda")}" href="?page={quote("Beranda")}" target="_self">Beranda</a>
    <a class="{nav_class("Katalog Produk")}" href="?page={quote("Katalog Produk")}" target="_self">Katalog Produk</a>
    <a class="{nav_class("Tentang")}" href="?page={quote("Tentang")}" target="_self">Tentang</a>
</div>
''',
            unsafe_allow_html=True
        )

    st.markdown('</div>', unsafe_allow_html=True)