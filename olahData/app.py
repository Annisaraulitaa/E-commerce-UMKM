from reranking.hybrid_rerank import balanced_hybrid_search

OUT_COLS = [
    "id", "name", "url",
    "category_breadcrumb",
    "price_number", "discountPercentage",
    "ratingAverage", "shop_id", "shop_name",
    "shop_city", "shop_tier", "countSold",
    "name_clean", "has_promo", "umkm_label",
]

HYBRID_SCORE_COLS = [
    "bm25_score",
    "popularity_raw",
    "value_score",
    "base_score",
    "final_score"
]


if __name__ == "__main__":

    query = input("Masukkan query: ").strip()
    
    results = balanced_hybrid_search(query=query)

    # ubah label 1/0 menjadi UMKM/NON_UMKM agar mudah dibaca
    results = results.copy()
    results["umkm_label"] = results["umkm_label"].replace({
        1: "UMKM",
        0: "NON_UMKM"
    })

    # ambil hanya kolom yang benar-benar ada
    cols_to_show = [c for c in OUT_COLS if c in results.columns]
    cols_to_show += [c for c in HYBRID_SCORE_COLS if c in results.columns]

    final_output = results[cols_to_show].copy()

    print("\n=== HASIL HYBRID ===")
    print(final_output.to_string(index=False))

    final_output.to_csv("hybrid_results.csv", index=False, encoding="utf-8-sig")
    print("\nDisimpan: hybrid_results.csv")