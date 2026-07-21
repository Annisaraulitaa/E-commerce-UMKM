from pathlib import Path

import pandas as pd
import streamlit as st

from utils import (
    load_submissions,
    update_submission_status,
    delete_submission,
    get_image_path,
)


def clear_catalog_cache():
    """Reset cache katalog agar produk approved bisa muncul saat kembali ke Beranda."""
    keys_to_clear = [
        "catalog_initial_products",
        "result",
        "last_query",
    ]

    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]


def render_submission_image(row):
    image_path = get_image_path(row.get("image_local_path", ""))

    if image_path is not None:
        st.image(str(image_path), width=200)
        st.caption(str(image_path))
    else:
        st.warning("Foto tidak tersedia atau path gambar tidak ditemukan.")
        st.caption(f"Path dari CSV: {row.get('image_local_path', '')}")


def show_admin_page():
    with st.container(key="admin_page"):
        st.title("Kelola Pendaftaran Produk")

        df = load_submissions()

        if df.empty:
            st.warning("Belum ada data pendaftaran produk.")
            return

        if "status" not in df.columns:
            st.error("Kolom status tidak ditemukan pada data pendaftaran.")
            return

        status_series = df["status"].fillna("pending").astype(str).str.strip().str.lower()

        total = len(df)
        pending = int(status_series.eq("pending").sum())
        approved = int(status_series.eq("approved").sum())
        rejected = int(status_series.eq("rejected").sum())

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total", total)
        col2.metric("Pending", pending)
        col3.metric("Approved", approved)
        col4.metric("Rejected", rejected)

        st.divider()

        status_filter = st.selectbox(
            "Filter Status",
            ["Semua", "pending", "approved", "rejected"],
        )

        if status_filter == "Semua":
            filtered_df = df.copy()
        else:
            filtered_df = df[status_series.eq(status_filter)].copy()

        st.subheader("Daftar Pendaftaran Produk")

        header = st.columns([0.5, 2.5, 2, 1.5, 1, 2])
        header[0].markdown("**No**")
        header[1].markdown("**Produk**")
        header[2].markdown("**Toko**")
        header[3].markdown("**Lokasi**")
        header[4].markdown("**Status**")
        header[5].markdown("**Aksi**")

        for display_no, (idx, row) in enumerate(filtered_df.iterrows(), start=1):
            col_no, col_product, col_shop, col_location, col_status, col_action = st.columns(
                [0.5, 2.5, 2, 1.5, 1, 2]
            )

            with col_no:
                st.write(display_no)

            with col_product:
                st.write(row.get("product_name", "-"))

            with col_shop:
                st.write(row.get("shop_name", "-"))

            with col_location:
                st.write(row.get("city", "-"))

            status = str(row.get("status", "pending")).strip().lower()

            with col_status:
                if status == "approved":
                    st.success(status)
                elif status == "rejected":
                    st.error(status)
                else:
                    st.warning(status)

            with col_action:
                with st.expander("Detail"):
                    st.write(f"Pemilik: {row.get('owner_name', '-')}")
                    st.write(f"Email: {row.get('email', '-')}")
                    st.write(f"WhatsApp: {row.get('whatsapp', '-')}")
                    st.write(f"Kategori: {row.get('business_category', '-')}")
                    st.write(f"Harga: Rp {row.get('estimated_price', '-')}")
                    render_submission_image(row)

                if status == "pending":
                    approve, reject = st.columns(2)

                    with approve:
                        if st.button("✓", key=f"approve_{idx}"):
                            update_submission_status(idx, "approved")
                            clear_catalog_cache()
                            st.rerun()

                    with reject:
                        if st.button("✕", key=f"reject_{idx}"):
                            update_submission_status(idx, "rejected")
                            clear_catalog_cache()
                            st.rerun()

                elif status == "approved":
                    if st.button("Hapus", key=f"delete_{idx}"):
                        delete_submission(idx)
                        clear_catalog_cache()
                        st.rerun()