import streamlit as st
import pandas as pd
from pathlib import Path
import re

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Katalog UMKM", layout="wide")

# --- CUSTOM CSS (TEMA BIRU & NAVBAR MINIMALIS) ---
st.markdown("""
    <style>
    /* Navbar Style */
    .nav-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 10px 5%;
        background: white;
        border-bottom: 1px solid #f0f0f0;
        margin-bottom: 20px;
    }
    .nav-brand {
        display: flex;
        align-items: center;
        gap: 10px;
        font-weight: bold;
        color: #0D47A1;
        font-size: 20px;
    }

/* Override Button Streamlit agar terlihat seperti teks menu */
    div.stButton > button {
        background: none !important;
        border: none !important;
        color: #64748b !important;
        font-weight: 500 !important;
        padding: 0px 0px 4px 0px !important; /* Beri sedikit ruang di bawah untuk garis */
        font-size: 14px !important;
        transition: all 0.3s ease;
        line-height: 1.2 !important;
        vertical-align: middle !important;
    }

    div.stButton > button:hover {
        color: #0D47A1 !important;
    }

    /* Style khusus untuk tombol yang aktif - Tanpa Padding Berubah */
    .active-btn > div.stButton > button {
        color: #0D47A1 !important;
        font-weight: 700 !important;
        /* Gunakan box-shadow inset agar posisi teks tidak bergeser */
        box-shadow: inset 0 -2px 0 0 #0D47A1 !important;
        border-radius: 0 !important;
        text-decoration: none !important; /* Hapus underline agar tidak turun */
    }

    /* Hero Section (Beranda) */
    .hero-container {
        background: linear-gradient(135deg, #0D47A1 0%, #1976D2 100%);
        padding: 60px 10%; color: white; display: flex; align-items: center; gap: 40px; margin: 0 -5rem;
    }
    .hero-title { font-size: 42px; font-weight: 800; line-height: 1.2; margin-bottom: 20px; }
    
    /* Stats & Cards */
    .stats-grid { display: flex; justify-content: center; gap: 20px; margin-top: -40px; margin-bottom: 40px; }
    .stat-card {
        background: white; padding: 25px; border-radius: 15px; text-align: center;
        box-shadow: 0 10px 25px rgba(0,0,0,0.05); width: 220px; border: 1px solid #f0f0f0;
    }
    .stat-val { font-size: 28px; font-weight: 800; color: #0D47A1; }

    /* Product Card */
    .product-card {
        border: 1px solid #e5e7eb; border-radius: 12px; background: #ffffff;
        margin-bottom: 10px; overflow: hidden;
    }
    .img-container { position: relative; width: 100%; aspect-ratio: 4/3; }
    .img-container img { width: 100%; height: 100%; object-fit: cover; }
    .card-content { padding: 16px; }
    .price-now { font-size: 18px; font-weight: 700; color: #2E59D9; margin: 8px 0; }
    
    /* Tombol Biru (Detail & Beli) */
    .blue-action-btn > div.stButton > button {
        background-color: #2E59D9 !important;
        color: white !important;
        border-radius: 8px !important;
        padding: 8px 16px !important;
        width: 100% !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- INITIALIZATION & DATA ---
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Beranda"

def load_data():
    path = Path("output")
    files = sorted([f.name for f in path.glob("*_enriched.csv")])
    if files: return pd.read_csv(path / files[0])
    return pd.DataFrame()

def format_rp(val):
    if val is None:
        return "-"
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return "-"
        digits = re.sub(r"[^0-9]", "", val)
        if digits:
            val = int(digits)
        else:
            return val
    try:
        num = float(val)
    except (TypeError, ValueError):
        return "-"
    if pd.isna(num):
        return "-"
    return f"Rp {int(num):,}".replace(",", ".")

# --- RENDER SINGLE HEADER ---
def render_header():
    # Brand di kiri, Navigasi di kanan menggunakan columns
    c_brand, c_spacer, c_b, c_k, c_a, c_t = st.columns([2.5, 3.5, 1, 1.2, 0.8, 0.8])
    
    with c_brand:
        st.markdown('<div class="nav-brand">🛍️ Katalog UMKM</div>', unsafe_allow_html=True)
    
    # Logic penentuan class active
    page = st.session_state["current_page"]
    
    with c_b:
        if page == "Beranda": st.markdown('<div class="active-btn">', unsafe_allow_html=True)
        if st.button("Beranda", key="n_beranda"): st.session_state["current_page"] = "Beranda"; st.rerun()
        if page == "Beranda": st.markdown('</div>', unsafe_allow_html=True)
        
    with c_k:
        if page == "Katalog Produk" or page == "Detail Produk": st.markdown('<div class="active-btn">', unsafe_allow_html=True)
        if st.button("Katalog Produk", key="n_katalog"): st.session_state["current_page"] = "Katalog Produk"; st.rerun()
        if page == "Katalog Produk" or page == "Detail Produk": st.markdown('</div>', unsafe_allow_html=True)
        
    with c_a:
        if page == "Analitik": st.markdown('<div class="active-btn">', unsafe_allow_html=True)
        if st.button("Analitik", key="n_analitik"): st.session_state["current_page"] = "Analitik"; st.rerun()
        if page == "Analitik": st.markdown('</div>', unsafe_allow_html=True)
        
    with c_t:
        if page == "Tentang": st.markdown('<div class="active-btn">', unsafe_allow_html=True)
        if st.button("Tentang", key="n_tentang"): st.session_state["current_page"] = "Tentang"; st.rerun()
        if page == "Tentang": st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('<hr style="margin: 0 0 20px 0; opacity: 0.1;">', unsafe_allow_html=True)

# --- PAGES CONTENT ---

def show_beranda():
    st.markdown("""
        <div class="hero-container">
            <div style="flex:1">
                <h1 class="hero-title">Sistem Informasi Katalog<br>Produk UMKM</h1>
                <p style="font-size: 18px; opacity: 0.9; margin-bottom: 30px;">
                    Platform digital yang memfasilitasi penjualan produk UMKM dan Non-UMKM dengan sistem katalog modern.
                </p>
            </div>
            <div style="flex:1">
                <img src="https://images.unsplash.com/photo-1542838132-92c53300491e?auto=format&fit=crop&q=80&w=600" style="border-radius:20px; width:100%;">
            </div>
        </div>
        <div class="stats-grid">
            <div class="stat-card"><div class="stat-val">6+</div><div style="color:#64748b;">Produk Tersedia</div></div>
            <div class="stat-card"><div class="stat-val">6+</div><div style="color:#64748b;">Toko Terdaftar</div></div>
            <div class="stat-card"><div class="stat-val">4,000+</div><div style="color:#64748b;">Total Penjualan</div></div>
            <div class="stat-card"><div class="stat-val">4.7</div><div style="color:#64748b;">Rating Rata-rata</div></div>
        </div>
    """, unsafe_allow_html=True)
    
    _, col_btn, _ = st.columns([2, 1, 2])
    with col_btn:
        st.markdown('<div class="blue-action-btn">', unsafe_allow_html=True)
        if st.button("Buka Katalog Sekarang →", key="hero_go"): 
            st.session_state["current_page"] = "Katalog Produk"; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

def show_katalog(df):
    st.subheader("Katalog Produk")
    st.text_input("Cari produk...", placeholder="Cari nama produk...", key="search")
    st.write("")

    if df.empty:
        st.info("Produk tidak tersedia.")
        return

    df_view = df.reset_index(drop=True)
    if len(df_view) > 30:
        df_view = df_view.sample(n=30).reset_index(drop=True)

    per_row = 3
    total = len(df_view)
    for i in range(0, total, per_row):
        cols = st.columns(per_row)
        for j, col in enumerate(cols):
            if i + j < total:
                row = df_view.iloc[i + j]
                with col:
                    st.markdown(f"""
                        <div class="product-card">
                            <div class="img-container">
                                <img src="{row.get('mediaURL_image', '')}">
                                <div class="badge-best" style="position:absolute; top:10px; left:10px; background:#FFC400; padding:2px 8px; border-radius:4px; font-size:10px; font-weight:bold;">UMKM</div>
                            </div>
                            <div class="card-content">
                                <div style="font-size:11px; color:#64748b; text-transform:uppercase;">{str(row.get('category_breadcrumb','')).split('/')[-1]}</div>
                                <div style="font-weight:600; margin:5px 0; line-height:1.4;">{row.get('name','')}</div>
                                <div style="font-size:12px; color:#4b5563;">⭐ {row.get('ratingAverage',0)} ({int(row.get('countReview',0))} ulasan)</div>
                                <div class="price-now">{format_rp(row.get('price_number',0))}</div>
                                <div style="font-size:12px; color:#64748b;">🏪 {row.get('shop_name','')} | 📍 {row.get('shop_city','')}</div>
                                <div style="font-size:12px; color:#64748b; margin-top:5px;">📦 {int(row.get('countSold',0))} terjual</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                    st.markdown('<div class="blue-action-btn">', unsafe_allow_html=True)
                    if st.button("Lihat Detail", key=f"det_{i+j}"):
                        st.session_state["selected_item"] = row.to_dict()
                        st.session_state["current_page"] = "Detail Produk"
                        st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)

def show_detail(item):
    st.markdown('<div class="blue-action-btn" style="width:150px">', unsafe_allow_html=True)
    if st.button("← Kembali"): st.session_state["current_page"] = "Katalog Produk"; st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
    
    c_l, c_r = st.columns([1, 1.5])
    with c_l: st.image(item.get('mediaURL_image'), use_container_width=True)
    with c_r:
        st.caption(item.get('category_breadcrumb'))
        st.title(item.get('name'))
        discounted_price = format_rp(item.get('price_number'))
        if discounted_price == "-":
            st.markdown("### Harga tidak tersedia")
        else:
            st.markdown(f"### {discounted_price}")

        original_price = format_rp(item.get('price_original'))
        discount_val = item.get('discountPercentage')
        price_meta = []

        if original_price != "-" and original_price != discounted_price:
            price_meta.append(f"<span style='color:#94a3b8; text-decoration: line-through;'>{original_price}</span>")

        discount_label = None
        if discount_val is not None and not (isinstance(discount_val, float) and pd.isna(discount_val)):
            if isinstance(discount_val, str):
                cleaned = re.sub(r"[^0-9.]", "", discount_val)
                discount_val = cleaned if cleaned else None
            try:
                discount_number = float(discount_val)
                if discount_number > 0:
                    discount_label = f"-{int(discount_number)}%"
            except (TypeError, ValueError):
                pass

        if discount_label:
            price_meta.append(f"<span style='color:#dc2626; font-weight:600;'>{discount_label}</span>")

        if price_meta:
            st.markdown(" ".join(price_meta), unsafe_allow_html=True)
        st.write(f"Toko: {item.get('shop_name')} | Lokasi: {item.get('shop_city')}")
        st.write(f"Rating: ⭐ {item.get('ratingAverage')} | Terjual: {int(item.get('countSold',0))}")
        st.write("---")
        st.markdown('<div class="blue-action-btn">', unsafe_allow_html=True)
        product_url = item.get('url')
        if product_url:
            if hasattr(st, "link_button"):
                st.link_button("Beli Sekarang", product_url)
            else:
                st.markdown(f"[Beli Sekarang]({product_url})")
        else:
            st.button("Beli Sekarang", disabled=True)
        st.markdown('</div>', unsafe_allow_html=True)

# --- MAIN APP EXECUTION ---
render_header()
df_data = load_data()

current = st.session_state["current_page"]
if current == "Beranda": show_beranda()
elif current == "Katalog Produk": show_katalog(df_data)
elif current == "Detail Produk": show_detail(st.session_state.get("selected_item", {}))
elif current == "Analitik": st.info("Halaman Analitik")
elif current == "Tentang": st.info("Halaman Tentang")