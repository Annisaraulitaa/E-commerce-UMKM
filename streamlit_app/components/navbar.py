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
    padding: 12px 36px 12px 36px;
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
    box-shadow: -3px 1px 0 2px #eaf5ff;
}

.umkm-brand-icon .material-symbols-rounded {
    font-size: 21px !important;
    line-height: 1 !important;
    color: #ffffff !important;
}

.umkm-brand-title {
    font-size: 16px;
    font-weight: 900;
    color: #0a5e94;
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

.umkm-register-nav {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 32px;
    padding: 0 16px 0 14px;
    border-radius: 10px;
    background: #2263e5;
    color: #ffffff !important;
    text-decoration: none !important;
    font-size: 13px;
    font-weight: 600;
    box-shadow: 0 8px 18px rgba(37, 99, 235, 0.28);
}

.umkm-register-nav:hover {
    background: #1d4ed8;
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

    register_url = "?page=Beranda&register_product=1"

    navbar_html = (
        '<div class="umkm-navbar">'
        '<div class="umkm-navbar-inner">'
        '<a class="umkm-brand" href="?page=Beranda" target="_self">'
        '<div class="umkm-brand-icon">🛍️</div>'
        '<div>'
        '<div class="umkm-brand-title">Katalog Produk</div>'
        '<div class="umkm-brand-subtitle">Sistem Rekomendasi</div>'
        '</div>'
        '</a>'

        '<div class="umkm-nav-menu">'
        f'<a class="umkm-register-nav" href="{register_url}" target="_self">📝&nbsp; Daftar Produk</a>'
        '</div>'

        '</div>'
        '</div>'
    )

    st.markdown(css + navbar_html, unsafe_allow_html=True)