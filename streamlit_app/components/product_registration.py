from datetime import datetime
from pathlib import Path
import uuid

import pandas as pd
import streamlit as st


SUBMISSION_FILE = Path("data/product_submissions.csv")
UPLOAD_IMAGE_DIR = Path("data/submitted_product_images")


PROVINCES = [
    "Aceh", "Sumatera Utara", "Sumatera Barat", "Riau", "Kepulauan Riau",
    "Jambi", "Sumatera Selatan", "Bangka Belitung", "Bengkulu", "Lampung",
    "DKI Jakarta", "Jawa Barat", "Jawa Tengah", "DI Yogyakarta", "Jawa Timur",
    "Banten", "Bali", "Nusa Tenggara Barat", "Nusa Tenggara Timur",
    "Kalimantan Barat", "Kalimantan Tengah", "Kalimantan Selatan",
    "Kalimantan Timur", "Kalimantan Utara", "Sulawesi Utara",
    "Sulawesi Tengah", "Sulawesi Selatan", "Sulawesi Tenggara",
    "Sulawesi Barat", "Gorontalo", "Maluku", "Maluku Utara",
    "Papua", "Papua Barat",
]

CATEGORIES = [
    "Makanan & Minuman",
    "Fashion & Pakaian",
    "Kerajinan & Seni",
    "Kecantikan & Perawatan",
    "Pertanian & Peternakan",
    "Furnitur & Dekorasi",
    "Aksesoris & Perhiasan",
    "Rumah Tangga",
    "Kesehatan",
    "Elektronik",
    "Lainnya",
]


def save_uploaded_product_image(uploaded_file):
    """Simpan file gambar produk ke folder lokal dan kembalikan path relatifnya."""
    if uploaded_file is None:
        return ""

    UPLOAD_IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    original_path = Path(uploaded_file.name)
    extension = original_path.suffix.lower()

    if extension not in [".jpg", ".jpeg", ".png", ".webp"]:
        extension = ".jpg"

    safe_stem = "".join(
        char if char.isalnum() or char in ["-", "_"] else "_"
        for char in original_path.stem.lower()
    ).strip("_")

    if not safe_stem:
        safe_stem = "produk"

    safe_stem = safe_stem[:35]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_code = uuid.uuid4().hex[:8]

    filename = f"{timestamp}_{unique_code}_{safe_stem}{extension}"
    save_path = UPLOAD_IMAGE_DIR / filename

    with open(save_path, "wb") as file:
        file.write(uploaded_file.getbuffer())

    return str(save_path)


def save_product_submission(data):
    SUBMISSION_FILE.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "submission_id": f"SUB-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}",
        "submitted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending",
        **data,
    }

    new_df = pd.DataFrame([record])

    if SUBMISSION_FILE.exists():
        old_df = pd.read_csv(SUBMISSION_FILE)
        final_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    final_df.to_csv(SUBMISSION_FILE, index=False, encoding="utf-8-sig")


def open_registration_dialog_from_url():
    return st.query_params.get("register_product", "") == "1"


def clear_registration_query():
    st.query_params.clear()
    st.query_params["page"] = "Beranda"


def render_registration_css():
    st.markdown(
        """
        <style>
        /* ===== REGISTRATION DIALOG BASE ===== */
        div[data-testid="stDialog"] {
            padding-top: 20px !important;
        }

        div[data-testid="stDialog"] div[role="dialog"] {
            max-width: 760px !important;
            border-radius: 18px !important;
        }

        div[data-testid="stDialog"] div[data-testid="stForm"] {
            border: none !important;
            padding: 0 !important;
        }

        div[data-testid="stDialog"] div[data-testid="stVerticalBlock"] {
            gap: 0.65rem !important;
        }

        div[data-testid="stDialog"] label {
            font-size: 13px !important;
            font-weight: 700 !important;
            color: #111827 !important;
            margin-bottom: 4px !important;
        }

        .register-divider {
            height: 1px;
            background: #e5e7eb;
            margin: -18px 0 8px 0;
        }

        /* ===== SECTION TITLE ===== */
        .register-section-title {
            display: flex;
            align-items: center;
            gap: 8px;
            margin: 10px 0 8px 0;
            color: #2563eb;
            font-size: 14px;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: .02em;
        }

        .register-section-number {
            width: 24px;
            height: 24px;
            border-radius: 7px;
            background: #2563eb;
            color: #ffffff;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 12px;
            font-weight: 900;
            flex-shrink: 0;
        }

        /* ===== TEXT INPUT ===== */
        div[data-testid="stDialog"] div[data-baseweb="input"] {
            height: 42px !important;
            min-height: 42px !important;
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            box-shadow: none !important;
        }

        div[data-testid="stDialog"] div[data-testid="stTextInput"] {
            margin: 0 !important;
            max-width: none !important;
            width: 100% !important;
        }

        div[data-testid="stDialog"] div[data-testid="stTextInput"] input {
            height: 42px !important;
            min-height: 42px !important;
            padding-left: 14px !important;
            padding-right: 14px !important;
            background-image: none !important;
            font-size: 14px !important;
        }

        div[data-testid="stDialog"] div[data-testid="InputInstructions"] {
            display: none !important;
        }

        /* ===== TEXTAREA ===== */
        div[data-testid="stDialog"] textarea {
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            min-height: 82px !important;
            font-size: 14px !important;
            box-shadow: none !important;
        }

        /* ===== SELECTBOX ===== */
        div[data-testid="stDialog"] div[data-baseweb="select"] > div {
            min-height: 42px !important;
            border-radius: 10px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            box-shadow: none !important;
        }

        /* ===== FILE UPLOADER FOTO PRODUK ===== */
        div[data-testid="stDialog"] [data-testid="stFileUploader"] {
            margin: 0 !important;
        }

        /* Label "Foto Produk *" */
        div[data-testid="stDialog"] [data-testid="stFileUploader"] label {
            font-size: 13px !important;
            font-weight: 700 !important;
            color: #111827 !important;
            margin-bottom: 4px !important;
        }

        /* Kotak upload utama */
        div[data-testid="stDialog"] [data-testid="stFileUploaderDropzone"] {
            min-height: 92px !important;
            padding: 10px 16px 14px 16px !important;
            border-radius: 10px !important;
            border: 1px dashed #cbd5e1 !important;
            background: #ecf1f6 !important;
            color: #334155 !important;
        }

        /* Container instruksi upload */
        div[data-testid="stDialog"] [data-testid="stFileUploaderDropzoneInstructions"] {
            gap: 4px !important;
        }

        /* Teks "Drag and drop file here" */
        div[data-testid="stDialog"] [data-testid="stFileUploaderDropzoneInstructions"] span {
            font-size: 13px !important;
            font-weight: 500 !important;
            color: #3d485f !important;
            line-height: 1.2 !important;
        }

        /* Teks "Limit 200MB..." */
        div[data-testid="stDialog"] [data-testid="stFileUploaderDropzoneInstructions"] small {
            font-size: 11px !important;
            font-weight: 400 !important;
            color: #64748b !important;
            line-height: 1.2 !important;
        }

        /* Tombol Browse files */
        div[data-testid="stDialog"] [data-testid="stFileUploader"] [data-testid="stBaseButton-secondary"] {
            height: 34px !important;
            min-height: 34px !important;
            border-radius: 9px !important;
            border: 1px solid #d1d5db !important;
            background: #ffffff !important;
            color: #111827 !important;
            font-size: 13px !important;
            font-weight: 700 !important;
            padding: 0 14px !important;
        }

        /* Teks di dalam tombol Browse files */
        div[data-testid="stDialog"] [data-testid="stFileUploader"] [data-testid="stBaseButton-secondary"] p {
            font-size: 13px !important;
            font-weight: 700 !important;
            color: #111827 !important;
            margin: 0 !important;
            padding: 0 !important;
        }

        /* Hover tombol */
        div[data-testid="stDialog"] [data-testid="stFileUploader"] [data-testid="stBaseButton-secondary"]:hover {
            background: #eff6ff !important;
            border-color: #2563eb !important;
            color: #2563eb !important;
        }

        /* ===== UMKM STATIC OPTION ===== */
        .business-type-field {
            margin: 2px 0 18px 0;
        }

        .business-type-label {
            font-size: 15px;
            font-weight: 400;
            color: #111827;
            line-height: 1.2;
            margin: 0 0 6px 0;
        }

        .single-umkm-option {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            margin: 0;
            height: 24px;
            color: #111827;
        }

        .single-umkm-dot {
            width: 18px;
            height: 18px;
            border-radius: 999px;
            border: 5px solid #2563eb;
            display: inline-block;
            box-sizing: border-box;
            flex-shrink: 0;
        }

        .single-umkm-text {
            font-size: 15px;
            font-weight: 700;
            color: #111827;
            line-height: 1;
        }

        /* ===== NOTE ===== */
        .register-note {
            background: #fffbeb;
            border: 1px solid #facc15;
            color: #92400e;
            border-radius: 12px;
            padding: 13px 16px;
            font-size: 13px;
            line-height: 1.5;
            margin-top: 16px;
            margin-bottom: 22px;
        }

        /* ===== SUCCESS DIALOG ===== */
        .success-dialog-content {
            text-align: center;
            padding: 10px 4px 4px 4px;
        }

        .success-icon {
            width: 64px;
            height: 64px;
            border-radius: 999px;
            background: #d1fae5;
            color: #059669;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 36px;
            margin: 0 auto 18px auto;
        }

        .success-title {
            font-size: 22px;
            font-weight: 900;
            color: #111827;
            margin-bottom: 8px;
        }

        .success-message {
            font-size: 14px;
            color: #6b7280;
            line-height: 1.6;
            margin-bottom: 22px;
        }

        .success-summary-card {
            background: #f8fafc;
            border-radius: 14px;
            padding: 16px 18px;
            margin-bottom: 20px;
            font-size: 13px;
        }

        .success-summary-row {
            display: flex;
            justify-content: space-between;
            gap: 16px;
            margin-bottom: 8px;
        }

        .success-summary-row:last-child {
            margin-bottom: 0;
        }

        .success-summary-label {
            color: #64748b;
        }

        .success-summary-value {
            color: #111827;
            font-weight: 800;
            text-align: right;
        }

        .success-business-badge {
            background: #10b981;
            color: #ffffff;
            border-radius: 6px;
            padding: 3px 8px;
            font-size: 11px;
            font-weight: 900;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


@st.dialog("Daftarkan Produk Anda", width="medium")
def render_product_registration_dialog():
    render_registration_css()

    st.markdown(
        '<div class="register-divider"></div>',
        unsafe_allow_html=True,
    )

    with st.form("product_registration_form", clear_on_submit=False):
        st.markdown(
            '<div class="register-section-title"><span class="register-section-number">1</span> Informasi Pemilik Usaha</div>',
            unsafe_allow_html=True,
        )

        owner_name = st.text_input("Nama Lengkap *", placeholder="cth. Siti Rahma Wulandari")

        col1, col2 = st.columns(2)
        with col1:
            email = st.text_input("Email *", placeholder="email@contoh.com")
        with col2:
            whatsapp = st.text_input("No. WhatsApp *", placeholder="08xxxxxxxxxx")

        st.markdown(
            '<div class="register-section-title"><span class="register-section-number">2</span> Informasi Toko</div>',
            unsafe_allow_html=True,
        )

        shop_name = st.text_input("Nama Toko *", placeholder="cth. Batik Warisan Nusantara")

        col1, col2 = st.columns(2)
        with col1:
            city = st.text_input("Kota / Kabupaten *", placeholder="cth. Yogyakarta")
        with col2:
            province = st.selectbox("Provinsi *", ["Pilih Provinsi"] + PROVINCES)

        business_category = st.selectbox(
            "Kategori Usaha *",
            ["Pilih Kategori Usaha"] + CATEGORIES,
        )

        st.markdown(
            '<div class="register-section-title"><span class="register-section-number">3</span> Informasi Produk Utama</div>',
            unsafe_allow_html=True,
        )

        product_name = st.text_input(
            "Nama Produk Utama *",
            placeholder="cth. Batik Tulis Motif Parang Premium",
        )

        description = st.text_area(
            "Deskripsi Singkat",
            placeholder="Ceritakan keunggulan produk Anda, bahan baku, keunikan, dll.",
            height=90,
        )

        col1, col2 = st.columns(2)
        with col1:
            price = st.text_input("Estimasi Harga (Rp)", placeholder="cth. 150000")
        with col2:
            uploaded_image = st.file_uploader(
                "Foto Produk *",
                type=["jpg", "jpeg", "png", "webp"],
            )

        st.markdown(
            """
            <div class="business-type-field">
                <div class="business-type-label">Jenis Usaha *</div>
                <div class="single-umkm-option">
                    <span class="single-umkm-dot"></span>
                    <span class="single-umkm-text">UMKM</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        business_type = "UMKM"

        st.markdown(
            """
            <div class="register-note">
                <b>Catatan:</b> Data yang Anda daftarkan akan diverifikasi oleh tim admin sebelum ditampilkan di katalog.
            </div>
            """,
            unsafe_allow_html=True,
        )

        col_cancel, col_submit = st.columns([1, 1.7])

        with col_cancel:
            cancel = st.form_submit_button("Batal", use_container_width=True)

        with col_submit:
            submit = st.form_submit_button("Kirim Pendaftaran", use_container_width=True)

        if cancel:
            clear_registration_query()
            st.session_state.show_registration_success = False
            st.rerun()

        if submit:
            required_fields = {
                "Nama lengkap": owner_name,
                "Email": email,
                "No. WhatsApp": whatsapp,
                "Nama toko": shop_name,
                "Kota / Kabupaten": city,
                "Provinsi": "" if province == "Pilih Provinsi" else province,
                "Kategori usaha": "" if business_category == "Pilih Kategori Usaha" else business_category,
                "Nama produk utama": product_name,
                "Jenis usaha": business_type,
            }

            empty_fields = [
                name
                for name, value in required_fields.items()
                if not str(value).strip()
            ]

            if empty_fields:
                st.error("Mohon lengkapi field wajib: " + ", ".join(empty_fields))
                return

            if uploaded_image is None:
                st.error("Mohon unggah foto produk.")
                return

            image_local_path = save_uploaded_product_image(uploaded_image)

            submission_data = {
                "owner_name": owner_name,
                "email": email,
                "whatsapp": whatsapp,
                "shop_name": shop_name,
                "city": city,
                "province": province,
                "business_category": business_category,
                "product_name": product_name,
                "description": description,
                "estimated_price": price,
                "image_local_path": image_local_path,
                "image_url": "",
                "business_type": business_type,
            }

            save_product_submission(submission_data)

            st.session_state.last_product_submission = submission_data
            st.session_state.show_registration_success = True
            st.rerun()


@st.dialog("Pendaftaran Berhasil", width="small")
def render_registration_success_dialog():
    render_registration_css()

    data = st.session_state.get("last_product_submission", {})

    success_html = (
        '<div class="success-dialog-content">'
        '<div class="success-icon">✓</div>'
        '<div class="success-title">Pendaftaran Berhasil!</div>'
        '<div class="success-message">'
        f'Terima kasih, <b>{data.get("owner_name", "-")}</b>!<br>'
        'Data Anda telah kami terima. '
        'Tim kami akan menghubungi '
        f'<b>{data.get("email", "-")}</b> '
        'dalam 2–3 hari kerja untuk proses verifikasi.'
        '</div>'
        '</div>'
        '<div class="success-summary-card">'
        '<div class="success-summary-row">'
        '<span class="success-summary-label">Nama Toko</span>'
        f'<span class="success-summary-value">{data.get("shop_name", "-")}</span>'
        '</div>'
        '<div class="success-summary-row">'
        '<span class="success-summary-label">Kota</span>'
        f'<span class="success-summary-value">{data.get("city", "-")}, {data.get("province", "-")}</span>'
        '</div>'
        '<div class="success-summary-row">'
        '<span class="success-summary-label">Kategori</span>'
        f'<span class="success-summary-value">{data.get("business_category", "-")}</span>'
        '</div>'
        '<div class="success-summary-row">'
        '<span class="success-summary-label">Jenis Usaha</span>'
        '<span class="success-summary-value">'
        f'<span class="success-business-badge">{data.get("business_type", "-")}</span>'
        '</span>'
        '</div>'
        '</div>'
    )

    st.markdown(success_html, unsafe_allow_html=True)

    if st.button("Kembali ke Beranda", use_container_width=True):
        st.session_state.show_registration_success = False
        clear_registration_query()
        st.rerun()


def render_product_registration_flow():
    if st.session_state.get("show_registration_success", False):
        render_registration_success_dialog()
        return

    if open_registration_dialog_from_url():
        render_product_registration_dialog()
