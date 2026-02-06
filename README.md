# Sistem Rekomendasi Produk UMKM (Search-Based) - Tokopedia

## Deskripsi
Project penelitian: pengembangan sistem rekomendasi produk UMKM pada fitur pencarian e-commerce menggunakan hybrid content-based filtering + UMKM-aware re-ranking.

## Fitur Utama
- Scraping & preprocessing data produk
- Label UMKM / non-UMKM (rule-based tagging)
- Candidate retrieval dengan BM25
- Re-ranking (relevansi + kualitas + popularitas + bias UMKM)
- UI web

## Cara Menjalankan scraping (fixmain.py)
ada 4 yang harus diganti kalau ingin scraping kategori lain
1. "params_template": "...(diganti dengan 'params' pada 'payload' kategori tersebut)",
2. keyword="...(sesuai kategori yang diambil)"           contoh -> keyword="Kain Daerah",  
3. out_csv="output/(....)_enriched.csv",                 contoh -> out_csv="output/kainDaerah_enriched.csv",
4. state_path="output/state_(...).json",                 contoh -> state_path="output/state_kainDaerah.json",

## Dataset
Minimal 100.000 produk Tokopedia. 
Kolom: id, name, url, categoryBreadcrumbs, price, 
       imageUrl, countReview, ratingAverage, discountPercentage, 
       shop_id, shop_name, shop_city, labelGroup, countSold, isTopads.

## Evaluasi
Precision@K, Recall@K, NDCG, fairness/exposure UMKM.
