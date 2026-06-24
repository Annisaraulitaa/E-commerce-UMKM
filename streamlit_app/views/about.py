import streamlit as st


def render_about_page():
    st.markdown('<div class="section-heading">Tentang Sistem</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="section-desc">Penjelasan singkat mengenai sistem rekomendasi produk UMKM yang dikembangkan.</div>',
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div class="info-card">
            <h3 style="color:#0D47A1;margin-bottom:12px;">Sistem Rekomendasi Produk UMKM</h3>
            <p style="color:#334155;line-height:1.8;">
                Sistem ini dikembangkan sebagai prototype penelitian untuk menampilkan
                rekomendasi produk berbasis pencarian pada e-commerce.
            </p>
            <p style="color:#334155;line-height:1.8;">
                Sistem menggunakan pendekatan Hybrid Content-Based Filtering dengan BM25
                sebagai candidate retrieval, kemudian dilakukan UMKM-Aware Re-Ranking
                untuk meningkatkan visibilitas produk UMKM.
            </p>
            <p style="color:#334155;line-height:1.8;">
                Antarmuka sistem dirancang menyerupai website katalog produk modern dengan
                halaman beranda, katalog produk, detail produk, dan halaman tentang.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.write("")

    st.markdown("### Komponen Sistem")

    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(
            """
            <div class="info-card">
                <h4>Candidate Retrieval</h4>
                <p style="color:#64748b;">Mengambil kandidat produk relevan menggunakan BM25.</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c2:
        st.markdown(
            """
            <div class="info-card">
                <h4>UMKM-Aware Re-Ranking</h4>
                <p style="color:#64748b;">Memberikan prioritas visibilitas pada produk UMKM yang relevan.</p>
            </div>
            """,
            unsafe_allow_html=True
        )

    with c3:
        st.markdown(
            """
            <div class="info-card">
                <h4>Continuous Ranking</h4>
                <p style="color:#64748b;">Menampilkan produk secara bertahap melalui mekanisme muat lebih banyak.</p>
            </div>
            """,
            unsafe_allow_html=True
        )