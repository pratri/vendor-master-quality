"""rapidfuzz baseline: name and address similarity, combined into one score (0-100)."""

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

# token_set_ratio gives 100 for subsets ("RED INC" vs "RED SKY INC"), WRatio doesn't
NAME_SCORERS = {"token_set_ratio": fuzz.token_set_ratio, "WRatio": fuzz.WRatio}


def score_pairs(v: pd.DataFrame, pairs: pd.DataFrame) -> pd.DataFrame:
    a, b = pairs["a"].to_numpy(), pairs["b"].to_numpy()
    names, addrs = v["name_norm"].to_numpy(), v["address_norm"].to_numpy()
    out = pairs.copy()
    for label, scorer in NAME_SCORERS.items():
        out[f"name_{label}"] = process.cpdist(names[a], names[b], scorer=scorer, workers=-1)
    out["address_score"] = process.cpdist(addrs[a], addrs[b], scorer=fuzz.token_set_ratio,
                                          workers=-1)
    return out


def combine(scored: pd.DataFrame, name_scorer: str, name_weight: float) -> np.ndarray:
    return (name_weight * scored[f"name_{name_scorer}"]
            + (1 - name_weight) * scored["address_score"]).to_numpy()
