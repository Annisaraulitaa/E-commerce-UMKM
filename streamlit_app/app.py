import re
from pathlib import Path

import pandas as pd
import streamlit as st

from recommender import UMKMRecommender
from metrics import fairness_at_k, count_umkm_at_k


# ============================================================
# KONFIGURASI HALAMAN
# ============================================================

st.set_page_config(
    page_title="Sistem Rekomendasi Produk UMKM",
    layout="wide"
)


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>
    .main-title {
        font-size: 34px;
        font-weight: 800;
        color: #0D47A1;
        margin-bottom: 8px;
    }

    .subtitle {
        font-size: 16px;
        color: #64748b;
        margin-bottom: 24px;
    }

    .hero-container {
        background: linear-gradient(135deg, #0D47A1 0%, #1976D2 100%);
        padding: 45px 50px;
        border-radius: 22px;
        color: white;
        margin-bottom: 28px;
    }

    .hero-title {
        font-size: 38px;
        font-weight: 800;
        line-height: 1.2;
        margin-bottom: 14px;
    }

    .hero-text {
        font-size: 16px;
        opacity: 0.92;
        max-width: 850px;
    }

    .metric-card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        padding: 18px 20px;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
        margin-bottom: 18px;
    }

    .metric-label {
        color: #64748b;
        font-size: 13px;
        margin-bottom: 4px;
    }

    .metric-value {
        color: #0D47A1;
        font-size: 28px;
        font-weight: 800;
    }

    .product-card {
        border: 1px solid #e5e7eb;
        border-radius: 16px;
        background: #ffffff;
        overflow: hidden;
        margin-bottom: 22px;
        box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05);
    }

    .product-img {
        width: 100%;
        height: 210px;
        object-fit: cover;
        background: #f1f5f9;
    }

    .product-content {
        padding: 16px;
    }

    .product-name {
        font-size: 15px;
        font-weight: 700;
        line-height: 1.35;
        color: #111827;
        min-height: 42px;
        margin-bottom: 8px;
    }

    .product-price {
        font-size: 18px;
        font-weight: 800;
        color: #2E59D9;
        margin-bottom: 8px;
    }

    .product-meta {
        font-size: 12px;
        color: #64748b;
        margin-bottom: 5px;
    }

    .badge-umkm {
        display: inline-block;
        background: #DCFCE7;
        color: #166534;
        font-size: 11px;
        font-weight: 800;
        padding: 4px 8px;
        border-radius: 999px;
        margin-bottom: 8px;
    }

    .badge-non {
        display: inline-block;
        background: #F1F5F9;
        color: #475569;
        font-size: 11px;
        font-weight: 800;
        padding: 4px 8px;
        border-radius: 999px;
        margin-bottom: 8px;
    }

    .score-box {
        background: #F8FAFC;
        border-radius: 12px;
        padding: 10px;
        margin-top: 10px;
        font-size: 12px;
        color: #334155;
    }

    .section-title {
        font-size: 22px;
        font-weight: 800;
        color: #0f172a;
        margin-top: 12px;
        margin-bottom: 16px;
    }

    div.stButton > button {
        background-color: #2E59D9 !important;
        color: white !important;
        border-radius: 10px !important;
        border: none !important;
        padding: 9px 16px !important;
        font-weight: 700 !important;
        width: 100%;
    }

    div.stButton > button:hover {
        background-color: #0D47A1 !important;
        color: white !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)


# ============================================================
# FUNGSI BANTU
# ============================================================

@st.cache_data
def load_data():
    base_dir = Path(__file__).resolve().parent
    data_path = base_dir.parent / "olahData" / "retrieval" / "nondup_labeled_dataset(new).csv"

    df = pd.read_csv(data_path)

    df["umkm_binary"] = (
        df["umkm_label"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map({"UMKM": 1, "NON_UMKM": 0})
        .fillna(0)
        .astype(int)
    )

    return df


def get_image_path(image_local_path):
    if pd.isna(image_local_path) or str(image_local_path).strip() == "":
        return None

    base_dir = Path(__file__).resolve().parent
    project_dir = base_dir.parent

    image_relative_path = str(image_local_path).replace("\\", "/")
    image_path = project_dir / image_relative_path

    return image_path if image_path.exists() else None


def format_rp(value):
    if pd.isna(value):
        return "-"

    if isinstance(value, str):
        digits = re.sub(r"[^0-9]", "", value)
        if digits == "":
            return value
        value = int(digits)

    try:
        return f"Rp {int(float(value)):,}".replace(",", ".")
    except Exception:
        return "-"


def safe_int(value):
    try:
        if pd.isna(value):
            return 0
        return int(float(value))
    except Exception:
        return 0


def safe_float(value, digits=4):
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), digits)
    except Exception:
        return 0.0


def render_metric_card(label, value):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_product_card(row, rank):
    image_path = get_image_path(row.get("image_local_path"))

    st.caption(f"Path gambar: {image_path}")

    with st.container(border=True):
        if image_path is not None:
            st.image(str(image_path), use_container_width=True)
        else:
            st.markdown(
                """
                <div style="
                    height: 210px;
                    background:#f1f5f9;
                    border-radius:12px;
                    display:flex;
                    align-items:center;
                    justify-content:center;
                    color:#94a3b8;
                    font-weight:600;
                ">
                    Gambar tidak tersedia
                </div>
                """,
                unsafe_allow_html=True
            )

        umkm_label = str(row.get("umkm_label", "-")).upper()

        if umkm_label == "UMKM":
            st.markdown(
                "<span style='background:#DCFCE7;color:#166534;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:800;'>UMKM</span>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                "<span style='background:#F1F5F9;color:#475569;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:800;'>NON-UMKM</span>",
                unsafe_allow_html=True
            )

        st.caption(f"Peringkat #{rank}")

        name = row.get("name", "-")
        price = format_rp(row.get("price_number", row.get("price", 0)))
        rating = row.get("ratingAverage", "-")
        sold = safe_int(row.get("countSold", 0))
        review = safe_int(row.get("countReview", 0))
        shop = row.get("shop_name", "-")
        city = row.get("shop_city", "-")

        relevance = safe_float(row.get("relevance_score", 0))
        popularity = safe_float(row.get("popularity_score", 0))
        value_score = safe_float(row.get("value_score", 0))
        final_score = safe_float(row.get("final_score", 0))

        st.markdown(f"**{name}**")
        st.markdown(
            f"<div style='font-size:20px;font-weight:800;color:#2E59D9;margin:6px 0;'>{price}</div>",
            unsafe_allow_html=True
        )

        st.caption(f"⭐ {rating} | {review} ulasan | 📦 {sold} terjual")
        st.caption(f"🏪 {shop}")
        st.caption(f"📍 {city}")

        st.markdown(
            f"""
            <div style="
                background:#F8FAFC;
                border-radius:12px;
                padding:10px;
                margin-top:10px;
                font-size:13px;
                color:#334155;
            ">
                Relevance: {relevance}<br>
                Popularity: {popularity}<br>
                Value: {value_score}<br>
                <b>Final Score: {final_score}</b>
            </div>
            """,
            unsafe_allow_html=True
        )


# ============================================================
# LOAD DATA DAN MODEL
# ============================================================

df = load_data()
recommender = UMKMRecommender(df)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Pengaturan Rekomendasi")

top_k = st.sidebar.slider(
    "Jumlah rekomendasi",
    min_value=5,
    max_value=50,
    value=10,
    step=5
)

weight_relevance = st.sidebar.slider(
    "Bobot Relevance",
    min_value=0.0,
    max_value=1.0,
    value=0.50,
    step=0.05
)

weight_popularity = st.sidebar.slider(
    "Bobot Popularity",
    min_value=0.0,
    max_value=1.0,
    value=0.20,
    step=0.05
)

weight_value = st.sidebar.slider(
    "Bobot Value",
    min_value=0.0,
    max_value=1.0,
    value=0.20,
    step=0.05
)

weight_umkm = st.sidebar.slider(
    "Bobot UMKM Boost",
    min_value=0.0,
    max_value=1.0,
    value=0.10,
    step=0.05
)


# ============================================================
# HALAMAN UTAMA
# ============================================================

st.markdown(
    """
    <div class="hero-container">
        <div class="hero-title">Sistem Rekomendasi Produk UMKM</div>
        <div class="hero-text">
            Prototype sistem rekomendasi berbasis pencarian menggunakan Hybrid Content-Based Filtering,
            BM25 Candidate Retrieval, dan UMKM-Aware Re-Ranking untuk meningkatkan visibilitas produk UMKM.
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

query = st.text_input(
    "Masukkan kata kunci pencarian produk:",
    placeholder="Contoh: baju batik wanita, kopi khas daerah, kerajinan anyaman"
)

search_clicked = st.button("Cari Rekomendasi")


if search_clicked:
    if query.strip() == "":
        st.warning("Masukkan query terlebih dahulu.")
    else:
        result = recommender.search(
            query=query,
            top_n=top_k,
            weight_relevance=weight_relevance,
            weight_popularity=weight_popularity,
            weight_value=weight_value,
            weight_umkm=weight_umkm
        )

        if result.empty:
            st.warning("Tidak ada hasil rekomendasi.")
        else:
            umkm_flags = result["umkm_binary"].astype(int).to_numpy()

            fairness = fairness_at_k(umkm_flags, k=top_k)
            umkm_count = count_umkm_at_k(umkm_flags, k=top_k)
            non_umkm_count = len(result) - umkm_count

            st.markdown('<div class="section-title">Ringkasan Hasil Rekomendasi</div>', unsafe_allow_html=True)

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                render_metric_card("Jumlah Produk", len(result))

            with col2:
                render_metric_card("Fairness@K", round(fairness, 3))

            with col3:
                render_metric_card("Produk UMKM", umkm_count)

            with col4:
                render_metric_card("Produk Non-UMKM", non_umkm_count)

            st.markdown('<div class="section-title">Daftar Rekomendasi Produk</div>', unsafe_allow_html=True)

            per_row = 3
            result_view = result.reset_index(drop=True)

            for i in range(0, len(result_view), per_row):
                cols = st.columns(per_row)

                for j, col in enumerate(cols):
                    idx = i + j

                    if idx < len(result_view):
                        with col:
                            render_product_card(result_view.iloc[idx], rank=idx + 1)

            with st.expander("Lihat tabel detail skor rekomendasi"):
                display_cols = [
                    "name",
                    "price_number",
                    "ratingAverage",
                    "countSold",
                    "countReview",
                    "shop_name",
                    "shop_city",
                    "umkm_label",
                    "image_local_path",
                    "relevance_score",
                    "popularity_score",
                    "value_score",
                    "final_score"
                ]

                available_cols = [col for col in display_cols if col in result.columns]

                st.dataframe(
                    result[available_cols],
                    use_container_width=True
                )

            st.markdown('<div class="section-title">Komposisi UMKM dan Non-UMKM</div>', unsafe_allow_html=True)

            chart_df = pd.DataFrame({
                "Kategori": ["UMKM", "Non-UMKM"],
                "Jumlah": [umkm_count, non_umkm_count]
            })

            st.bar_chart(chart_df.set_index("Kategori"))