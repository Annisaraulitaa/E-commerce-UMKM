import pandas as pd
import streamlit as st
from pathlib import Path

from components.navbar import render_navbar
from config import INITIAL_DISPLAY, VALID_PAGES
from data_loader import load_data
from recommender import UMKMRecommender
from styles import load_css
from views.about import render_about_page
from views.catalog import render_catalog_page
from views.detail import render_detail_product
from views.admin import show_admin_page
from views.submitted_detail import render_submitted_product_detail
from submitted_products import load_approved_submitted_products

st.set_page_config(
    page_title="Sistem Rekomendasi Produk UMKM",
    layout="wide",
    initial_sidebar_state="collapsed",
)

load_css()

df = load_data()

approved_products = load_approved_submitted_products()

st.write("APP PATH:")
st.write(Path.cwd())

st.write("SUBMISSION FILE EXISTS:")
st.write(Path("data/product_submissions.csv").exists())

st.write("SUBMISSION RAW:")
st.write(
    pd.read_csv(
        "data/product_submissions.csv",
        dtype=str
    ).head()
)

if not approved_products.empty:
    approved_signature = "|".join(
        approved_products["id"].astype(str).str.strip().tolist()
    )

    if st.session_state.get("approved_products_signature") != approved_signature:
        st.session_state["approved_products_signature"] = approved_signature
        st.session_state.pop("catalog_initial_products", None)

recommender = UMKMRecommender(df)

catalog_df = df.copy()

if not approved_products.empty:
    catalog_df = pd.concat(
        [
            approved_products,
            catalog_df
        ],
        ignore_index=True
    )


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

if "product_id" in st.query_params:
    product_id = str(st.query_params.get("product_id", "")).strip()

    route_df = catalog_df.copy()

    route_df["_route_id"] = (
        route_df["id"]
        .astype(str)
        .str.strip()
    )

    matched_product = route_df[
        route_df["_route_id"] == product_id
    ]
    
    if not matched_product.empty:
        st.session_state.selected_product = matched_product.iloc[0].to_dict()
        st.session_state.current_page = "Detail Produk"
    else:
        st.session_state.selected_product = None
        st.session_state.current_page = "Beranda"
        st.query_params.clear()
        st.query_params["page"] = "Beranda"
        st.session_state.pop("catalog_initial_products", None)
        st.rerun()

if st.session_state.current_page not in [
    "Detail Produk",
    "Admin"
]:
    render_navbar()

st.write("DEBUG PAGE:", st.session_state.current_page)
st.write("DEBUG PRODUCT:", st.session_state.selected_product)

if st.session_state.current_page == "Beranda":
    render_catalog_page(catalog_df, recommender)

elif st.session_state.current_page == "Detail Produk":
    selected_product = st.session_state.selected_product

    if selected_product and selected_product.get("source") == "submission":
        render_submitted_product_detail(selected_product)
    else:
        render_detail_product(selected_product)

elif st.session_state.current_page == "Tentang":
    render_about_page()

elif st.session_state.current_page == "Admin":
    show_admin_page()