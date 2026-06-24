import numpy as np
import pandas as pd
from rank_bm25 import BM25Okapi

from preprocessing_streamlit import tokenize


class UMKMRecommender:
    def __init__(self, df):
        self.df = df.copy()

        self.df["search_text"] = (
            self.df["name"].fillna("").astype(str) + " " +
            self.df["category_breadcrumb"].fillna("").astype(str) + " " +
            self.df["shop_city"].fillna("").astype(str)
        )

        self.tokenized_corpus = self.df["search_text"].apply(tokenize).tolist()
        self.bm25 = BM25Okapi(self.tokenized_corpus)

    def normalize_score(self, series):
        min_val = series.min()
        max_val = series.max()

        if max_val == min_val:
            return np.zeros(len(series))

        return (series - min_val) / (max_val - min_val)

    def search(
        self,
        query,
        top_n=10,
        weight_relevance=0.50,
        weight_popularity=0.20,
        weight_value=0.20,
        weight_umkm=0.10,
        first_umkm_quota=60
    ):
        query_tokens = tokenize(query)

        if len(query_tokens) == 0:
            return pd.DataFrame()

        bm25_scores = self.bm25.get_scores(query_tokens)

        result = self.df.copy()
        result["bm25_score"] = bm25_scores

        result = result[result["bm25_score"] > 0]

        if result.empty:
            return pd.DataFrame()

        result = result.sort_values(
            "bm25_score",
            ascending=False
        ).head(top_n)

        result["relevance_score"] = self.normalize_score(result["bm25_score"])

        result["popularity_raw"] = (
            result["countSold"].fillna(0) +
            result["countReview"].fillna(0)
        )

        result["popularity_score"] = self.normalize_score(result["popularity_raw"])

        result["value_raw"] = (
            result["ratingAverage"].fillna(0) +
            result["discountPercentage"].fillna(0) / 100
        )

        result["value_score"] = self.normalize_score(result["value_raw"])

        result["umkm_score"] = result["umkm_binary"].astype(int)

        total_weight = (
            weight_relevance +
            weight_popularity +
            weight_value +
            weight_umkm
        )

        if total_weight == 0:
            total_weight = 1

        result["final_score"] = (
            weight_relevance * result["relevance_score"] +
            weight_popularity * result["popularity_score"] +
            weight_value * result["value_score"] +
            weight_umkm * result["umkm_score"]
        ) / total_weight

        # Urutkan semua kandidat berdasarkan final_score
        result = result.sort_values("final_score", ascending=False)

        # Ambil produk UMKM yang relevan untuk mengisi 60 produk pertama
        umkm_first = result[result["umkm_binary"] == 1].head(first_umkm_quota)

        # Ambil sisa produk selain yang sudah masuk 60 pertama
        remaining = result.drop(index=umkm_first.index)

        # Gabungkan:
        # 1. UMKM relevan di posisi awal
        # 2. sisanya campuran UMKM dan NON-UMKM berdasarkan final_score
        result = pd.concat([umkm_first, remaining], axis=0)

        # Batasi sesuai top_n / seluruh produk relevan
        result = result.head(top_n)

        return result