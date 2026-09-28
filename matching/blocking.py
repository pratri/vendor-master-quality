"""Blocking: only compare vendors that share a cheap key, instead of all 200M pairs.

Pass 1: state + first three digits of the ZIP code (same local area).
Pass 2: first token of the normalized name (catches the same company at another address).
Blocks above max_block are skipped; they are generic tokens like COUNTY or UNIVERSITY that
would add many pairs and few matches. Skipped blocks are reported.
"""

import numpy as np
import pandas as pd

MAX_BLOCK = 1000


def block_keys(v: pd.DataFrame) -> pd.DataFrame:
    """Two blocking keys per vendor; blank when the input is missing."""
    zip3 = v["zip5"].str[:3]
    area = np.where((v["state"] != "") & (zip3.str.len() == 3), v["state"] + "|" + zip3, "")
    first = v["name_norm"].str.split(" ").str[0].fillna("")
    return pd.DataFrame({"area": area, "first_token": first}, index=v.index)


def candidate_pairs(v: pd.DataFrame, max_block: int = MAX_BLOCK) -> tuple[pd.DataFrame, dict]:
    """Unique (a, b) row-position pairs with a < b, and blocking stats."""
    keys = block_keys(v)
    parts, stats = [], {"skipped_blocks": []}
    for pass_name in ("area", "first_token"):
        groups = pd.Series(np.arange(len(v))).groupby(keys[pass_name].values)
        n_pairs = 0
        for key, idx in groups:
            members = idx.to_numpy()
            if key == "" or len(members) < 2:
                continue
            if len(members) > max_block:
                stats["skipped_blocks"].append((pass_name, key, len(members)))
                continue
            a, b = np.triu_indices(len(members), k=1)
            parts.append(np.column_stack([members[a], members[b]]))
            n_pairs += len(a)
        stats[f"pairs_{pass_name}"] = n_pairs
    pairs = np.unique(np.vstack(parts), axis=0) if parts else np.empty((0, 2), dtype=int)
    stats["pairs_total"] = len(pairs)
    return pd.DataFrame(pairs, columns=["a", "b"]), stats
