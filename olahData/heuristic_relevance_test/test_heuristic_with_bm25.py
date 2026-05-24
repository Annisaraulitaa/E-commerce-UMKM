from retrieval.bm25 import bm25_candidates, bm25_search
from heuristic_relevance import add_relevance_labels, evaluate_one_query_heuristic

query = "kopi khas daerah"
K = 40
TOP_N = 2000

# Ambil candidate pool BM25
candidate_pool = bm25_candidates(query, top_n=TOP_N)

# Ambil hasil Top-K BM25
bm25_result = bm25_search(query, topk=K, use_term_filter=True)

# Tambahkan label relevansi pada hasil BM25
bm25_labeled = add_relevance_labels(bm25_result, query)

print("\n=== HASIL BM25 + LABEL RELEVANSI ===")
print(
    bm25_labeled[
        ["name", "category_breadcrumb", "umkm_label", "bm25_score", "relevance_label"]
    ].head(20).to_string(index=False)
)

# Hitung metrik
metrics = evaluate_one_query_heuristic(
    df_result=bm25_result,
    df_candidates=candidate_pool,
    query=query,
    k=K
)

print("\n=== METRIK BM25 ===")
for key, value in metrics.items():
    print(f"{key}: {value}")