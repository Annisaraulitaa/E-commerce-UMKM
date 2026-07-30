import numpy as np
import pandas as pd
from rank_bm25 import BM25Okapi

from config import (
    TOP_N_CANDIDATES,
    TOP_K_RESULTS,
    WEIGHT_RELEVANCE,
    WEIGHT_PERFORMANCE,
    WEIGHT_VALUE,
    PERFORMANCE_SOLD_WEIGHT,
    PERFORMANCE_REVIEW_WEIGHT,
    PERFORMANCE_TOTAL_RATING_WEIGHT,
    VALUE_RATING_WEIGHT,
    VALUE_DISCOUNT_WEIGHT,
    BM25_K1,
    BM25_B
)

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
        self.bm25 = BM25Okapi(
            self.tokenized_corpus,
            k1=BM25_K1,
            b=BM25_B
        )

    def normalize_score(self, series):
        series = pd.to_numeric(series, errors="coerce").fillna(0)

        min_val = series.min()
        max_val = series.max()

        if max_val == min_val:
            return np.zeros(len(series))

        return (series - min_val) / (max_val - min_val)

    def safe_numeric(self, df, col):
        if col not in df.columns:
            return pd.Series([0] * len(df), index=df.index)

        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    def search(
        self,
        query,
        top_n=None,
        top_n_candidates=TOP_N_CANDIDATES,
        top_k_results=TOP_K_RESULTS,
        weight_relevance=WEIGHT_RELEVANCE,
        weight_popularity=WEIGHT_PERFORMANCE,
        weight_value=WEIGHT_VALUE,
        popularity_sold_weight=PERFORMANCE_SOLD_WEIGHT,
        popularity_review_weight=PERFORMANCE_REVIEW_WEIGHT,
        popularity_total_rating_weight=PERFORMANCE_TOTAL_RATING_WEIGHT,
        value_rating_weight=VALUE_RATING_WEIGHT,
        value_discount_weight=VALUE_DISCOUNT_WEIGHT,
    ):
        if top_n is not None:
            top_k_results = top_n

        query_tokens = tokenize(query)

        if len(query_tokens) == 0:
            return pd.DataFrame()

        bm25_scores = self.bm25.get_scores(query_tokens)

        result = self.df.copy()
        result["bm25_score"] = bm25_scores

        result = result[result["bm25_score"] > 0]

        if result.empty:
            return pd.DataFrame()

        # Candidate retrieval dari BM25
        result = result.sort_values(
            "bm25_score",
            ascending=False
        ).head(top_n_candidates)

        # =====================================================
        # 1. Relevance Score
        # =====================================================
        result["relevance_score"] = self.normalize_score(result["bm25_score"])

        # =====================================================
        # 2. Popularity Score
        # countSold lebih besar karena lebih mewakili transaksi/minat pasar
        # =====================================================
        count_sold = self.safe_numeric(result, "countSold")
        count_review = self.safe_numeric(result, "countReview")
        total_rating = self.safe_numeric(result, "totalRating")

        result["countSold_norm"] = self.normalize_score(np.log1p(count_sold))
        result["countReview_norm"] = self.normalize_score(np.log1p(count_review))
        result["totalRating_norm"] = self.normalize_score(np.log1p(total_rating))

        result["popularity_score"] = (
            popularity_sold_weight * result["countSold_norm"] +
            popularity_review_weight * result["countReview_norm"] +
            popularity_total_rating_weight * result["totalRating_norm"]
        )

        # =====================================================
        # 3. Value Score
        # ratingAverage lebih besar karena lebih mewakili kualitas produk
        # =====================================================
        rating = self.safe_numeric(result, "ratingAverage")
        discount = self.safe_numeric(result, "discountPercentage")

        result["rating_norm"] = self.normalize_score(rating)
        result["discount_norm"] = self.normalize_score(discount)

        result["value_score"] = (
            value_rating_weight * result["rating_norm"] +
            value_discount_weight * result["discount_norm"]
        )

        # =====================================================
        # 4. Base Score
        # relevance_dominant: alpha=0.50, beta=0.25, gamma=0.25
        # =====================================================
        result["base_score"] = (
            weight_relevance * result["relevance_score"] +
            weight_popularity * result["popularity_score"] +
            weight_value * result["value_score"]
        )

        # =====================================================
        # 5. Final Score
        # =====================================================
        result["final_score"] = (
            result["base_score"]
        )

        # Urutkan kandidat berdasarkan final_score
        result = result.sort_values("final_score", ascending=False)

        result = result.head(top_k_results).reset_index(drop=True)
        result["final_rank"] = np.arange(1, len(result) + 1)

        return result