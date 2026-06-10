import numpy as np


def fairness_at_k(umkm_flags: np.ndarray, k: int) -> float:
    eff_k = min(k, len(umkm_flags))

    if eff_k == 0:
        return np.nan

    return float(umkm_flags[:eff_k].sum() / eff_k)


def count_umkm_at_k(umkm_flags: np.ndarray, k: int) -> int:
    eff_k = min(k, len(umkm_flags))

    if eff_k == 0:
        return 0

    return int(umkm_flags[:eff_k].sum())