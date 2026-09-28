"""Evaluate the matcher against the hidden UEI answer key.

Positives: every pair of vendor records sharing a UEI, whether blocking found it or not, so
recall includes what blocking loses. Negatives: candidate pairs with different UEIs and no
parent link. Related: different UEIs under the same parent (or parent and child); these are
distinct companies, so they are neither positives nor negatives and are reported apart.
Random pairs are never sampled: nearly all would be easy non-matches and flatter precision.
"""

import numpy as np
import pandas as pd


def positive_pairs(labels: pd.DataFrame) -> pd.DataFrame:
    """All (LIFNR_A, LIFNR_B) pairs, A < B, that share a UEI."""
    m = labels.merge(labels, on="uei", suffixes=("_A", "_B"))
    m = m[m["LIFNR_A"] < m["LIFNR_B"]]
    return m[["LIFNR_A", "LIFNR_B"]].reset_index(drop=True)


def label_pairs(pairs: pd.DataFrame, labels: pd.DataFrame) -> pd.Series:
    lab = labels.set_index("LIFNR")
    a = lab.loc[pairs["LIFNR_A"]].reset_index(drop=True)
    b = lab.loc[pairs["LIFNR_B"]].reset_index(drop=True)
    same_parent = (a["parent_uei"] != "") & (a["parent_uei"] == b["parent_uei"])
    parent_child = (a["parent_uei"] == b["uei"]) | (b["parent_uei"] == a["uei"])
    out = np.where(a["uei"] == b["uei"], "match",
                   np.where(same_parent | parent_child, "related", "non_match"))
    return pd.Series(out, index=pairs.index)


def fold(pairs: pd.DataFrame) -> np.ndarray:
    """Deterministic 50/50 dev/test split per pair: 0 = dev (tuning), 1 = test (reported)."""
    key = pairs["LIFNR_A"].astype(str) + "|" + pairs["LIFNR_B"].astype(str)
    return (pd.util.hash_pandas_object(key, index=False).to_numpy() % 2).astype(int)


def pr_curve(score: np.ndarray, label: np.ndarray, n_positive: int,
             thresholds=range(40, 101)) -> pd.DataFrame:
    rows = []
    is_match, is_neg = label == "match", label == "non_match"
    for t in thresholds:
        hit = score >= t
        tp, fp = int((hit & is_match).sum()), int((hit & is_neg).sum())
        precision = tp / (tp + fp) if tp + fp else 1.0
        recall = tp / n_positive if n_positive else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        rows.append((t, tp, fp, precision, recall, f1))
    return pd.DataFrame(rows, columns=["threshold", "tp", "fp", "precision", "recall", "f1"])
