import numpy as np
import pandas as pd

from utils import normalize_umkm_label


def precision_at_k(labels, k):
    labels = labels[:k]
    if k == 0:
        return 0.0
    return float(np.sum(labels) / total_relevant)


def recall_at_k(labels, total_relevant, k):
    labels = labels[:k]
    if total_relevant == 0:
        return 0.0
    return float(np.sum(labels) / total_relevant)


def f1_score(p, r):
    if p + r == 0:
        return 0.0
    return float(2 * p * r / (p + r))


def ndcg_at_k(labels, total_relevant, k):
    labels = np.array(labels[:k], dtype=float)

    if len(labels) == 0 or k == 0:
        return 0.0

    weights = 1 / np.log2(np.arange(2, len(labels) + 2))
    dcg = float(np.sum(labels * weights))

    ideal_relevant_count = min(int(total_relevant), k)

    if ideal_relevant_count == 0:
        return 0.0

    ideal = np.array(
        [1] * ideal_relevant_count + [0] * (len(labels) - ideal_relevant_count),
        dtype=float
    )

    idcg = float(np.sum(ideal * weights))

    if idcg == 0:
        return 0.0

    return float(dcg / idcg)


def fairness_at_k(df, k):
    df = normalize_umkm_label(df)
    df_k = df.head(k)

    if len(df_k) == 0:
        return 0.0

    return float(df_k["umkm_label"].mean())


def unjudged_at_k(df_labeled, k):
    df_k = df_labeled.head(k)

    if len(df_k) == 0:
        return 0.0

    return float(1 - df_k["judged"].mean())