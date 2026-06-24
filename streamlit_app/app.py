import pandas as pd
import streamlit as st

from components.navbar import render_navbar
from config import VALID_PAGES
from data_loader import load_data
from views.about import render_about_page
from views.catalog import render_catalog_page
from views.detail import render_detail_product
from views.home import render_home_page
from recommender import UMKMRecommender
from styles import load_css


st.set_page_config(
    page_title="Sistem Rekomendasi Produk UMKM",
    layout="wide"
)

load_css()

df = load_data()
recommender = UMKMRecommender(df)

if "visible_count" not in st.session_state:
    st.session_state.visible_count = 60

if "last_query" not in st.session_state:
    st.session_state.last_query = ""

if "result" not in st.session_state:
    st.session_state.result = pd.DataFrame()

if "current_page" not in st.session_state:
    st.session_state.current_page = "Beranda"

if "page" in st.query_params:
    page_from_url = st.query_params.get("page")

    if page_from_url in VALID_PAGES:
        st.session_state.current_page = page_from_url

if "selected_product" not in st.session_state:
    st.session_state.selected_product = None

if st.session_state.current_page != "Detail Produk":
    render_navbar()

if st.session_state.current_page == "Beranda":
    render_home_page(df)

elif st.session_state.current_page == "Katalog Produk":
    render_catalog_page(df, recommender)

elif st.session_state.current_page == "Detail Produk":
    render_detail_product(st.session_state.selected_product)

elif st.session_state.current_page == "Tentang":
    render_about_page()