# =========================================================
# HEURISTIC GROUND TRUTH RELEVANCE
# =========================================================
# File ini digunakan untuk menentukan apakah produk relevan
# terhadap query berdasarkan aturan:
# 1) kategori/teks produk sesuai dengan intent query
# 2) keyword wajib muncul
# 3) tidak mengandung keyword pengecualian
#
# Output relevansi:
# 1 = relevan
# 0 = tidak relevan
# =========================================================

import re
import math
import numpy as np
import pandas as pd


# =========================================================
# QUERY INTENTS
# =========================================================
# Sesuaikan daftar ini dengan query eksperimen Anda.
# Tambahkan query baru jika diperlukan.
# Catatan:
# - category_keywords: kata yang diharapkan muncul pada kategori/teks produk
# - must_have_keywords: kata wajib/utama yang harus muncul
# - optional_keywords: kata pendukung intent query
# - exclude_keywords: kata yang membuat produk tidak relevan
# =========================================================

QUERY_INTENTS = {
    "kopi khas daerah": {
    "category_keywords": [
        "kopi", "kopi bubuk", "minuman", "makanan minuman"
    ],

    # Keyword wajib cukup "kopi", jangan tambah "khas" dan "daerah"
    "must_have_keywords": [
        "kopi"
    ],

    "must_match_mode": "all",

    # Optional adalah indikator bahwa produk ini benar-benar kopi khas/lokal/produk kopi utama
    "optional_keywords": [
        # bentuk produk kopi
        "bubuk", "biji", "roasted", "roast", "arabika", "robusta",
        "liberika", "single origin",

        # kata lokal/khas
        "khas", "daerah", "lokal",

        # nama daerah/kopi lokal
        "gayo", "aceh", "toraja", "lampung", "flores",
        "kintamani", "mandailing", "pontianak", "pagaralam",
        "bali", "papua", "java", "jawa", "sumatera", "sulawesi"
    ],

    "exclude_keywords": [
        # alat/perlengkapan kopi
        "gelas", "mug", "cangkir", "mesin", "alat", "grinder",
        "dripper", "filter", "tamper", "sendok", "sedotan",

        # makanan/snack rasa kopi, bukan produk kopi utama
        "keripik", "kripik", "snack", "cemilan", "camilan",
        "biskuit", "wafer", "permen", "kue", "roti",
        "cookies", "bolu", "brownies", "coklat", "cokelat"
    ],

    "min_optional_match": 1,
    "strict_optional": True
},

    "kopi instan sachet": {
        "category_keywords": ["kopi", "makanan", "minuman"],
        "must_have_keywords": ["kopi"],
        "optional_keywords": ["instan", "sachet", "stick", "renteng", "3in1", "3 in 1"],
        "exclude_keywords": [
            "gelas", "mug", "cangkir", "mesin", "alat",
            "sedotan", "filter", "dripper"
        ],
        "min_optional_match": 1
    },

    "baju batik pria": {
        "category_keywords": ["batik", "fashion", "pakaian", "baju", "kemeja", "pria"],
        "must_have_keywords": ["batik"],
        "optional_keywords": ["pria", "laki", "cowok", "kemeja", "baju", "atasan", "formal"],
        "exclude_keywords": ["wanita", "perempuan", "cewek", "dress", "rok", "gamis", "daster", "anak", "bayi"],
        "min_optional_match": 1
    },

    "tas wanita kulit": {
        "category_keywords": ["tas", "fashion", "wanita", "bag"],
        "must_have_keywords": ["tas"],
        "optional_keywords": ["wanita", "perempuan", "cewek", "kulit", "leather", "lokal", "handmade", "sling", "selempang", "handbag", "totebag", "shoulder"],
        "exclude_keywords": ["gantungan", "strap", "tali", "dompet", "pouch", "cover", "case", "aksesoris"],
        "min_optional_match": 1
    },

    "hiasan rumah handmade": {
        "category_keywords": ["hiasan", "dekorasi", "rumah", "home", "dekor", "kerajinan"],
        "must_have_keywords": ["hiasan", "dekorasi", "pajangan"],
        "optional_keywords": ["rumah", "handmade", "lokal", "kerajinan", "kayu", "rotan", "anyaman", "dinding"],
        "exclude_keywords": ["baju", "tas", "makanan", "minuman", "sepatu"],
        "min_optional_match": 1
    },

    "oleh-oleh khas daerah": {
        "category_keywords": ["makanan", "snack", "cemilan", "oleh", "khas"],
        "must_have_keywords": ["oleh", "khas", "daerah", "makanan", "snack", "cemilan"],
        "optional_keywords": [
            "daerah", "lokal", "tradisional", "khas", "dodol", "keripik",
            "bakpia", "pia", "pempek", "abon", "sambal", "rendang"
        ],
        "exclude_keywords": ["baju", "tas", "sepatu", "mainan", "hiasan", "alat"],
        "min_optional_match": 1
    },

    "keripik singkong": {
        "category_keywords": ["makanan", "snack", "cemilan", "keripik"],
        "must_have_keywords": ["keripik", "singkong"],
        "optional_keywords": ["pedas", "balado", "original", "renyah", "gurih", "lokal", "umkm"],
        "exclude_keywords": ["alat", "mesin"],
        "min_optional_match": 0
    },
}


# =========================================================
# TEXT UTILITIES
# =========================================================

def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokens(text):
    return clean_text(text).split()


def contains_any(text, keywords):
    text = clean_text(text)
    if not keywords:
        return False
    return any(clean_text(keyword) in text for keyword in keywords)


def count_keyword_match(text, keywords):
    text = clean_text(text)
    if not keywords:
        return 0
    return sum(1 for keyword in keywords if clean_text(keyword) in text)


def get_value(row, possible_cols):
    for col in possible_cols:
        if col in row.index:
            return row.get(col, "")
    return ""


def build_product_text(row):
    """
    Menggabungkan atribut produk untuk evaluasi relevansi.
    Dibuat fleksibel karena nama kolom pada dataset/kode bisa berbeda.
    """
    name = get_value(row, ["name_clean", "name", "product_name"])
    category = get_value(row, ["category_clean", "category_breadcrumb", "categoryBreadcrumbs", "category"])
    city = get_value(row, ["city_clean", "shop_city"])
    shop_name = get_value(row, ["shop_name"])

    return clean_text(f"{name} {name} {category} {city} {shop_name}")


def build_category_text(row):
    category = get_value(row, ["category_clean", "category_breadcrumb", "categoryBreadcrumbs", "category"])
    return clean_text(category)


# =========================================================
# HEURISTIC RELEVANCE
# =========================================================

def fallback_relevance(query, row):
    """
    Fallback jika query belum ada di QUERY_INTENTS.
    Aturan:
    - Query 1-2 token: semua token harus muncul
    - Query 3 token: minimal 2 token muncul
    - Query >3 token: minimal n-1 token muncul, minimum 3
    """
    q_tokens = tokens(query)

    if not q_tokens:
        return 0

    product_text = build_product_text(row)
    product_tokens = set(tokens(product_text))

    n_tokens = len(q_tokens)

    if n_tokens <= 2:
        min_match = n_tokens
    elif n_tokens == 3:
        min_match = 2
    else:
        min_match = max(3, n_tokens - 1)

    match_count = sum(token in product_tokens for token in q_tokens)

    return int(match_count >= min_match)


def is_relevant_product(query, row, query_intents=None):
    """
    Ground truth heuristic relevance.

    Produk dianggap relevan jika:
    - tidak mengandung exclude keyword
    - keyword wajib muncul
    - kategori sesuai ATAU optional intent cukup kuat

    Return:
    1 = relevan
    0 = tidak relevan
    """
    if query_intents is None:
        query_intents = QUERY_INTENTS

    query_clean = clean_text(query)
    intent = query_intents.get(query_clean)

    if intent is None:
        return fallback_relevance(query_clean, row)

    product_text = build_product_text(row)
    category_text = build_category_text(row)

    exclude_match = contains_any(product_text, intent.get("exclude_keywords", []))
    if exclude_match:
        return 0

    must_keywords = intent.get("must_have_keywords", [])
    optional_keywords = intent.get("optional_keywords", [])
    category_keywords = intent.get("category_keywords", [])
    min_optional_match = intent.get("min_optional_match", 1)

    # Untuk must_have:
    # - Jika hanya 1 keyword, keyword itu harus muncul.
    # - Jika lebih dari 1 keyword, minimal salah satu muncul.
    must_match = contains_any(product_text, must_keywords)

    category_match = (
        contains_any(category_text, category_keywords)
        or contains_any(product_text, category_keywords)
    )

    optional_count = count_keyword_match(product_text, optional_keywords)
    optional_match = optional_count >= min_optional_match

    # Aturan utama: kategori cocok + keyword wajib cocok
    if category_match and must_match:
        return 1

    # Aturan cadangan: keyword wajib cocok + optional kuat
    if must_match and optional_match:
        return 1

    return 0


def add_relevance_labels(df, query, label_col="relevance_label"):
    df = df.copy()
    if len(df) == 0:
        df[label_col] = []
        return df

    df[label_col] = df.apply(
        lambda row: is_relevant_product(query, row),
        axis=1
    ).astype(int)

    return df


def relevant_mask_df(df, query):
    if len(df) == 0:
        return pd.Series([False] * len(df), index=df.index)

    return df.apply(
        lambda row: bool(is_relevant_product(query, row)),
        axis=1
    )


# =========================================================
# METRICS
# =========================================================

def precision_at_k(df, query, k):
    df_k = add_relevance_labels(df.head(k), query)

    if len(df_k) == 0:
        return 0.0

    return float(df_k["relevance_label"].sum() / len(df_k))


def recall_at_k(df_topk, df_all, query, k):
    df_all_labeled = add_relevance_labels(df_all, query)
    total_relevant = int(df_all_labeled["relevance_label"].sum())

    if total_relevant == 0:
        return 0.0

    df_topk_labeled = add_relevance_labels(df_topk.head(k), query)
    retrieved_relevant = int(df_topk_labeled["relevance_label"].sum())

    return float(retrieved_relevant / total_relevant)


def f1_at_k(precision, recall):
    if precision + recall == 0:
        return 0.0
    return float(2 * precision * recall / (precision + recall))


def ndcg_at_k(df, query, k):
    df_k = add_relevance_labels(df.head(k), query)

    if len(df_k) == 0:
        return 0.0

    rel = df_k["relevance_label"].astype(int).to_numpy()

    discounts = 1 / np.log2(np.arange(2, len(rel) + 2))
    dcg = float(np.sum(rel * discounts))

    ideal_rel = np.sort(rel)[::-1]
    idcg = float(np.sum(ideal_rel * discounts))

    if idcg == 0:
        return 0.0

    return float(dcg / idcg)


def prepare_umkm_label(df):
    df = df.copy()

    if "umkm_label" not in df.columns:
        df["umkm_label"] = 0

    df["umkm_label"] = df["umkm_label"].replace({
        "UMKM": 1,
        "NON_UMKM": 0,
    })

    df["umkm_label"] = pd.to_numeric(
        df["umkm_label"],
        errors="coerce"
    ).fillna(0).astype(int)

    return df


def fairness_at_k(df, k):
    df = prepare_umkm_label(df)
    df_k = df.head(k)

    if len(df_k) == 0:
        return 0.0

    return float(df_k["umkm_label"].mean())


def evaluate_one_query_heuristic(df_result, df_candidates, query, k):
    """
    Fungsi evaluasi utama yang bisa dipakai oleh BM25, Hybrid, dan tuning.
    """
    precision = precision_at_k(df_result, query, k)
    recall = recall_at_k(df_result, df_candidates, query, k)
    f1 = f1_at_k(precision, recall)
    ndcg = ndcg_at_k(df_result, query, k)
    fairness = fairness_at_k(df_result, k)

    df_labeled = add_relevance_labels(df_result.head(k), query)

    return {
        "Precision": precision,
        "Recall": recall,
        "F1_score": f1,
        "NDCG": ndcg,
        "Fairness": fairness,
        "Relevant@K": int(df_labeled["relevance_label"].sum()),
        "Total_Relevant_Candidates": int(add_relevance_labels(df_candidates, query)["relevance_label"].sum()),
    }
