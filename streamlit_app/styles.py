import streamlit as st


def load_css():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
            padding-left: 3rem;
            padding-right: 3rem;
            padding-bottom: 3rem;
            max-width: 1480px;
        }

        body {
            background: #f8fafc;
        }

        .nav-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 14px 18px;
            margin-top: 18px;
            margin-bottom: 28px;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
        }

        .brand-area {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-icon {
            width: 46px;
            height: 46px;
            border-radius: 14px;
            background: #2563eb;
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 23px;
            font-weight: 800;
        }

        .brand-title {
            font-size: 23px;
            font-weight: 850;
            color: #0f172a;
            line-height: 1.1;
        }

        .brand-subtitle {
            font-size: 13px;
            color: #64748b;
            margin-top: 3px;
        }

        .nav-menu {
            display: flex;
            justify-content: flex-end;
            align-items: center;
            gap: 14px;
        }

        .nav-link {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 12px 18px;
            border-radius: 12px;
            color: #334155 !important;
            text-decoration: none !important;
            font-size: 16px;
            font-weight: 750;
            transition: 0.2s ease;
        }

        .nav-link:hover {
            background: #f1f5f9;
            color: #0f172a !important;
        }

        .nav-link.active {
            background: #2563eb;
            color: #ffffff !important;
            box-shadow: 0 10px 22px rgba(37, 99, 235, 0.22);
        }

        .hero-section {
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
            border-radius: 28px;
            padding: 58px;
            color: white;
            margin-bottom: 34px;
            box-shadow: 0 18px 40px rgba(37, 99, 235, 0.22);
        }

        .hero-title {
            font-size: 46px;
            font-weight: 900;
            line-height: 1.12;
            margin-bottom: 18px;
        }

        .hero-desc {
            font-size: 18px;
            line-height: 1.75;
            opacity: 0.94;
            max-width: 900px;
        }

        .hero-actions {
            display: flex;
            align-items: center;
            gap: 16px;
            margin-top: 34px;
        }

        .hero-primary-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: #ffffff;
            color: #2563eb !important;
            padding: 15px 28px;
            border-radius: 12px;
            text-decoration: none !important;
            font-size: 16px;
            font-weight: 850;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.18);
        }

        .hero-primary-btn:hover {
            background: #eff6ff;
            color: #1d4ed8 !important;
        }

        .hero-secondary-btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: transparent;
            color: #ffffff !important;
            padding: 14px 28px;
            border-radius: 12px;
            border: 2px solid rgba(255,255,255,0.9);
            text-decoration: none !important;
            font-size: 16px;
            font-weight: 850;
        }

        .hero-secondary-btn:hover {
            background: rgba(255,255,255,0.12);
        }

        .section-heading {
            font-size: 30px;
            font-weight: 900;
            color: #0f172a;
            margin-top: 34px;
            margin-bottom: 8px;
        }

        .section-desc {
            font-size: 16px;
            color: #64748b;
            margin-bottom: 24px;
        }

        .info-card {
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 24px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
        }

        .green-card {
            background: #f0fdf4;
            border: 1px solid #bbf7d0;
            border-radius: 20px;
            padding: 28px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.04);
        }

        .blue-card {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            border-radius: 20px;
            padding: 28px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.04);
        }

        .detail-category {
            padding: 18px 20px;
            color: #475569;
            font-size: 14px;
            border-top: 1px solid #e5e7eb;
            border-bottom: 1px solid #e5e7eb;
            background: #ffffff;
        }

        .description-section b {
            color: #0f172a;
            font-weight: 850;
        }

        .search-panel {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 22px;
            margin-bottom: 28px;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
        }

        div[data-testid="stTextInput"] input {
            border-radius: 12px !important;
            border: 1px solid #cbd5e1 !important;
            padding: 14px 16px !important;
            font-size: 15px !important;
            background: #ffffff !important;
        }

        div.stButton > button {
            border-radius: 12px !important;
            background-color: #2563eb !important;
            color: white !important;
            border: none !important;
            font-weight: 750 !important;
            padding: 10px 16px !important;
            min-height: 42px;
            box-shadow: 0 8px 18px rgba(37, 99, 235, 0.18);
        }

        div.stButton > button:hover {
            background-color: #1d4ed8 !important;
            color: white !important;
        }

        .metric-mini {
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 16px 18px;
            background: #ffffff;
            box-shadow: 0 6px 18px rgba(15,23,42,0.05);
            min-height: 92px;
        }

        .metric-mini-label {
            font-size: 12px;
            color: #64748b;
            margin-bottom: 6px;
        }

        .metric-mini-value {
            font-size: 26px;
            font-weight: 850;
            color: #2563eb;
        }

        .result-panel {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 20px 22px;
            margin-bottom: 22px;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.05);
        }

        .result-title {
            font-size: 18px;
            font-weight: 850;
            color: #0f172a;
            margin-bottom: 4px;
        }

        .result-subtitle {
            color: #64748b;
            font-size: 14px;
        }

        .sort-box {
            background:#f8fafc;
            border:1px solid #e5e7eb;
            border-radius:14px;
            padding:14px 18px;
            margin:22px 0;
            color:#334155;
            font-size:14px;
            display:flex;
            justify-content:space-between;
            align-items:center;
        }

        .product-image-box {
            width: 100%;
            height: 260px;
            border-radius: 14px;
            overflow: hidden;
            background: #f1f5f9;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 14px;
        }

        .product-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
        }

        /* Jika gambar tidak tersedia */
        .product-image-empty {
            color: #94a3b8;
            font-size: 14px;
            font-weight: 700;
        }

        /* Isi card dibuat tinggi tetap agar tombol sejajar */
        .product-card-body {
            min-height: 285px;
        }

        .product-card-top {
            height: 32px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }

        .product-badge {
            color: white;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 850;
        }

        .product-rank {
            color: #94a3b8;
            font-size: 12px;
            font-weight: 800;
        }

        .product-name {
            font-size: 15px;
            font-weight: 800;
            color: #0f172a;
            line-height: 1.45;
            height: 46px;
            overflow: hidden;

            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;

            text-overflow: ellipsis;
            margin-top: 0px;
            margin-bottom: 14px;
        }

        .product-price {
            height: 34px;
            font-size: 22px;
            font-weight: 900;
            color: #2563eb;
            margin-bottom: 12px;
        }

        .product-meta {
            height: 28px;
            font-size: 14px;
            color: #64748b;
            line-height: 28px;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }

        .product-location {
            margin-bottom: 14px;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 18px !important;
            border-color: #e5e7eb !important;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.05);
            background: white;
            height: 100%;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            height: 100%;
        }

        div[data-testid="stVerticalBlockBorderWrapper"]:hover {
            border-color: #2563eb !important;
            box-shadow: 0 10px 26px rgba(15, 23, 42, 0.10);
        }

        button[kind="secondary"] {
            background: #ffffff !important;
            color: #334155 !important;
            border: 1px solid #e5e7eb !important;
            box-shadow: none !important;
        }

        /* Tombol detail dibuat konsisten */
        div.stButton > button {
            min-height: 48px !important;
            border-radius: 12px !important;
            font-weight: 800 !important;
        }

        /* ===== DETAIL PRODUCT - UKURAN LEBIH PROPORSIONAL ===== */
        .detail-main {
            padding: 22px 20px 24px 20px !important;
        }

        .detail-image-box {
            height: 360px !important;
            background: #f8fafc;
            border-radius: 16px;
            overflow: hidden;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 1px solid #e5e7eb;
        }

        .detail-image-box img {
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
            background: #ffffff;
        }

        .detail-title {
            font-size: 28px !important;
            line-height: 1.28 !important;
            color: #0f172a;
            font-weight: 900;
            margin-bottom: 16px !important;
        }

        .detail-meta {
            font-size: 14px !important;
            color: #475569;
            margin-bottom: 18px !important;
        }

        .detail-price-box {
            background: #f8fafc;
            border-radius: 16px;
            padding: 18px 20px !important;
            margin-bottom: 18px !important;
        }

        .detail-original-price {
            font-size: 14px !important;
            color: #94a3b8;
            text-decoration: line-through;
            margin-bottom: 4px;
        }

        .detail-price {
            font-size: 30px !important;
            font-weight: 900;
            color: #2563eb;
            margin-bottom: 4px;
        }

        .detail-saving {
            font-size: 13px !important;
            color: #16a34a;
            font-weight: 600;
        }

        .detail-badge-row {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
            margin-bottom: 14px !important;
        }

        .shop-box {
            padding: 16px 18px !important;
            margin-bottom: 18px !important;
        }

        .shop-name {
            font-size: 16px !important;
            font-weight: 850;
            color: #0f172a;
            margin-bottom: 4px;
        }

        .shop-location {
            font-size: 13px !important;
            color: #64748b;
        }

        .description-title {
            font-size: 22px !important;
            font-weight: 900;
            color: #0f172a;
            margin-bottom: 14px;
        }

        .description-section p {
            font-size: 15px !important;
            line-height: 1.75 !important;
            color: #334155;
        }

        /* ===== SEARCH INPUT & BUTTON ALIGNMENT ===== */
        div[data-testid="stTextInput"] {
            margin-bottom: 0 !important;
        }

        div[data-testid="stTextInput"] > div {
            height: 44px !important;
        }

        div[data-testid="stTextInput"] input {
            height: 44px !important;
            min-height: 44px !important;
            padding: 0 16px !important;
            box-sizing: border-box !important;
            border-radius: 11px !important;
            font-size: 14px !important;
        }

        div[data-testid="stButton"] {
            margin-bottom: 0 !important;
        }

        div[data-testid="stButton"] button {
            height: 44px !important;
            min-height: 44px !important;
            padding: 0 12px !important;
            box-sizing: border-box !important;
            border-radius: 11px !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            font-size: 14px !important;
            font-weight: 800 !important;
        }

        /* ======= ADMIN PAGE ======= */

        .st-key-admin_page {
            padding-top: 20px;
        }

        /* ===== MOBILE NAVBAR ===== */
        @media (max-width: 700px) {

            .umkm-navbar-inner {
                flex-direction: row !important;
                align-items: center !important;
                justify-content: space-between !important;
                padding: 8px 12px !important;
                gap: 12px !important;
                width: 100% !important;
            }

            .umkm-nav-menu {
                justify-content: flex-end !important;
                width: auto !important;
            }

            .umkm-brand {
                flex: 1 1 auto !important; /* Brand fleksibel */
            }

            .umkm-register-nav {
                flex-shrink: 0 !important;
                padding: 6px 12px !important;
                font-size: 13px !important;
            }

            /* CTA Daftar Produk Sekarang */
            .catalog-register-cta {
                flex-direction: row !important;
                align-items: center !important;
                justify-content: space-between !important;
                gap: 8px !important;
                width: 100% !important;
                padding: 12px 8px !important;
            }

            .catalog-register-title {
                font-size: 14px !important;
                line-height: 1.3 !important;
            }

            .catalog-register-subtitle {
                font-size: 12px !important;
            }

            .catalog-register-button {
                height: 36px !important;
                padding: 0 14px !important;
                font-size: 13px !important;
                flex-shrink: 0 !important;
            }

            /* Info Bar Menampilkan Produk */
            .catalog-info-bar {
                flex-direction: row !important;
                align-items: center !important;
                justify-content: space-between !important;
                gap: 6px;
                margin-top: 0px !important;     
                padding-top: 2px !important;    
                padding-bottom: 2px !important;
            }

            .catalog-grid-mobile {
                display: grid !important;
                grid-template-columns: 1fr; 
                row-gap: 12px;             
                column-gap: 12px; 
                padding-left: 8px !important;
                padding-right: 8px !important;          
            }

            .catalog-info-left,
            .catalog-info-right {
                white-space: nowrap !important;
                flex-wrap: nowrap !important;
            }

            .st-key-new_umkm_grid_wrap {
                margin-top: 0px !important;       
                padding-top: 0px !important;
            }

            /* Container vertical block wrapper internal Streamlit */
            div[data-testid="stVerticalBlock"][data-key="new_umkm_grid_wrap"] {
                margin-top: 0px !important;
                padding-top: 0px !important;
            }

            div[data-testid="stVerticalBlockBorderWrapper"] {
                margin-top: 0px !important;
                padding-top: 0px !important;
            }

            /* Konsistensi gap antar produk */
            .catalog-grid-mobile {
                display: grid !important;
                grid-template-columns: 1fr !important;
                row-gap: 12px !important;
                column-gap: 0 !important;
                padding-left: 0 !important;
                padding-right: 0 !important;
            }

            .st-key-initial_product_grid_wrap,
            .st-key-search_result_grid_wrap,
            .st-key-new_umkm_grid_wrap {
                width: 100% !important;
                max-width: 100% !important;
                padding-left: 10px !important;
                padding-right: 10px !important;
                margin-left: auto !important;
                margin-right: auto !important;
                box-sizing: border-box !important;
            }
            
            .st-key-new_umkm_grid_wrap .catalog-info-bar {
                margin-top: 4px !important;
                margin-bottom: 2px !important;
                padding-top: 2px !important;
                padding-bottom: 2px !important;
            }

        }

        </style>

        """,
        unsafe_allow_html=True
    )