import pandas as pd
import streamlit as st

from components.navbar import render_navbar
from config import INITIAL_DISPLAY, VALID_PAGES
from data_loader import load_data
from recommender import UMKMRecommender
from styles import load_css
from views.about import render_about_page
from views.catalog import render_catalog_page
from views.detail import render_detail_product


st.set_page_config(
    page_title="Sistem Rekomendasi Produk UMKM",
    layout="wide",
    initial_sidebar_state="collapsed",
)

load_css()

df = load_data()
recommender = UMKMRecommender(df)

if "visible_count" not in st.session_state:
    st.session_state.visible_count = INITIAL_DISPLAY

if "last_query" not in st.session_state:
    st.session_state.last_query = ""

if "result" not in st.session_state:
    st.session_state.result = pd.DataFrame()

if "current_page" not in st.session_state:
    st.session_state.current_page = "Beranda"

if "selected_product" not in st.session_state:
    st.session_state.selected_product = None

# Routing halaman biasa dari navbar.
if "page" in st.query_params:
    page_from_url = st.query_params.get("page")

    if page_from_url in VALID_PAGES:
        st.session_state.current_page = page_from_url

# Routing detail produk dari klik card.
# Product card mengirim product_id lewat query params supaya detail tetap bisa dibuka
# meskipun card dibuat clickable seperti desain Figma.
if "product_id" in st.query_params:
    product_id = str(st.query_params.get("product_id"))
    matched_product = df[df["id"].astype(str) == product_id]

    if not matched_product.empty:
        st.session_state.selected_product = matched_product.iloc[0].to_dict()
        st.session_state.current_page = "Detail Produk"

if st.session_state.current_page != "Detail Produk":
    render_navbar()

if st.session_state.current_page == "Beranda":
    render_catalog_page(df, recommender)

elif st.session_state.current_page == "Detail Produk":
    render_detail_product(st.session_state.selected_product)

elif st.session_state.current_page == "Tentang":
    render_about_page()
