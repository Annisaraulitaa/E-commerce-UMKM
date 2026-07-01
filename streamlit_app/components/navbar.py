from urllib.parse import quote

import streamlit as st


def render_navbar():
    current = st.session_state.current_page

    def nav_class(page_name):
        return "umkm-nav-link active" if current == page_name else "umkm-nav-link"

    css = """
<style>
.umkm-navbar {
    width: 100%;
    background: #ffffff;
    border-bottom: 1px solid #e5e7eb;
    margin-bottom: 0px;
}

.umkm-navbar-inner {
    max-width: 1380px;
    margin: 0 auto;
    padding: 16px 8px 30px 36px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 24px;
}

.umkm-brand {
    display: flex;
    align-items: center;
    gap: 11px;
    text-decoration: none !important;
}

.umkm-brand-icon {
    width: 38px;
    height: 38px;
    border-radius: 11px;
    background: #ffffff;
    color: #0041d1;
    border: 1px solid #ececec;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 18px;
    font-weight: 900;
    box-shadow: 0 4px 18px rgba(37, 99, 235, 0.14);
}

.umkm-brand-icon .material-symbols-rounded {
    font-size: 21px !important;
    line-height: 1 !important;
    color: #ffffff !important;
}

.umkm-brand-title {
    font-size: 17px;
    font-weight: 900;
    color: #2563eb;
    line-height: 1.05;
}

.umkm-brand-subtitle {
    font-size: 11px;
    color: #64748b;
    margin-top: 1px;
}

.umkm-nav-menu {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 12px;
}

.umkm-nav-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 38px;
    padding: 0 14px;
    border-radius: 24px;
    color: #173b5f !important;
    background: transparent;
    text-decoration: none !important;
    font-size: 12px;
    font-weight: 850;
    transition: all 0.18s ease;
}

.umkm-nav-link:hover {
    background: #f1f5f9;
    color: #0f172a !important;
}

.umkm-nav-link.active {
    background: transparent;
    color: #2563eb !important;
    box-shadow: 0 8px 18px rgba(37, 99, 235, 0.6);
}

@media (max-width: 700px) {
    .umkm-navbar-inner {
        flex-direction: column;
        align-items: flex-start;
    }

    .umkm-nav-menu {
        justify-content: flex-start;
    }
}
</style>
"""

    beranda_url = f"?page={quote('Beranda')}"
    tentang_url = f"?page={quote('Tentang')}"

    navbar_html = (
        '<div class="umkm-navbar">'
        '<div class="umkm-navbar-inner">'
        f'<a class="umkm-brand" href="{beranda_url}" target="_self">'
        '<div class="umkm-brand-icon">🛍️</div>'
        '<div>'
        '<div class="umkm-brand-title">Katalog Produk</div>'
        '<div class="umkm-brand-subtitle">Sistem Rekomendasi</div>'
        '</div>'
        '</a>'
        '<div class="umkm-nav-menu">'
        f'<a class="{nav_class("Beranda")}" href="{beranda_url}" target="_self">Beranda</a>'
        f'<a class="{nav_class("Tentang")}" href="{tentang_url}" target="_self">Tentang</a>'
        '</div>'
        '</div>'
        '</div>'
    )

    st.markdown(css + navbar_html, unsafe_allow_html=True)