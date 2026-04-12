from reranking.hybrid_rerank import balanced_hybrid_search

if __name__ == "__main__":
    
    query = input("Masukkan query: ")
    
    results = balanced_hybrid_search(
        query=query,
        top_n_candidates=2000,
        top_k_results=20,
        min_umkm_ratio=0.4
    )
    
    print(results[[
        "name",
        "shop_name",
        "umkm_label",
        "bm25_score",
        "final_score"
    ]])