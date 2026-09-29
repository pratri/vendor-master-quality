"""Hard-case pairs for manual labeling, and scoring of the matchers against those labels.

    python -m matching.hard_cases build   -> data/labels/hard_case_candidates.csv (+ _key.csv)
    python -m matching.hard_cases score   -> data/matching/hard_case_metrics.json

The UEI answer is kept in a separate key file so it doesn't bias labeling.
Labels: duplicate, related, not_duplicate, unsure (see docs/labeling_guide.md).
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from matching.__main__ import load_labels, load_vendors
from matching.baseline import combine, score_pairs
from matching.blocking import candidate_pairs
from matching.evaluate import label_pairs
from matching.splink_model import splink_scores

CANDIDATES = Path("data/labels/hard_case_candidates.csv")
KEY = Path("data/labels/hard_case_key.csv")
METRICS = Path("data/matching/hard_case_metrics.json")
SEED = 7
STRATA = {  # stratum -> number of pairs
    "same_name_different_uei": 45,
    "near_threshold": 45,
    "missed_true_pair": 35,
    "related_above_threshold": 25,
}


def scored_pairs() -> tuple[pd.DataFrame, pd.DataFrame]:
    v = load_vendors()
    pairs, _ = candidate_pairs(v)
    s = score_pairs(v, pairs)
    s["LIFNR_A"], s["LIFNR_B"] = v.LIFNR.to_numpy()[s.a], v.LIFNR.to_numpy()[s.b]
    s["uei_label"] = label_pairs(s[["LIFNR_A", "LIFNR_B"]], load_labels(v)).to_numpy()
    s = s.merge(splink_scores(v), on=["LIFNR_A", "LIFNR_B"], how="left")
    metrics = json.loads(Path("data/matching/metrics.json").read_text())
    c = metrics["chosen"]
    s["score"] = combine(s, c["name_scorer"], c["name_weight"])
    s["baseline_score"] = combine(s, "token_set_ratio",
                                  metrics["baseline_token_set_ratio"]["name_weight"])
    return s, v


def build() -> None:
    s, v = scored_pairs()
    t = json.loads(Path("data/matching/metrics.json").read_text())["chosen"]["threshold"]
    nn = v.name_norm.to_numpy()
    same_name = nn[s.a] == nn[s.b]
    pools = {
        "same_name_different_uei": s[same_name & (s.uei_label != "match")],
        "near_threshold": s[~same_name & s.score.between(t - 6, t + 6)],
        "missed_true_pair": s[(s.uei_label == "match") & (s.score < t)],
        "related_above_threshold": s[(s.uei_label == "related") & (s.score >= t)],
    }
    rng = np.random.default_rng(SEED)
    picks = []
    for name, pool in pools.items():
        idx = rng.choice(len(pool), size=min(STRATA[name], len(pool)), replace=False)
        picks.append(pool.iloc[np.sort(idx)].assign(stratum=name))
    hard = pd.concat(picks)
    hard = hard.iloc[rng.permutation(len(hard))].reset_index(drop=True)
    hard.insert(0, "pair_id", [f"H{i:03d}" for i in range(1, len(hard) + 1)])

    info = v.set_index("LIFNR")[["name", "address", "city", "state", "zip5"]]
    out = (hard[["pair_id", "LIFNR_A", "LIFNR_B"]]
           .join(info.add_suffix("_A"), on="LIFNR_A").join(info.add_suffix("_B"), on="LIFNR_B"))
    out["score"] = hard["score"].round(1)
    out["label"], out["notes"] = "", ""
    CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(CANDIDATES, index=False)
    hard[["pair_id", "LIFNR_A", "LIFNR_B", "stratum", "uei_label"]].to_csv(KEY, index=False)
    print(f"{len(out)} pairs written to {CANDIDATES}; strata:",
          hard.stratum.value_counts().to_dict())


def score() -> None:
    labeled = pd.read_csv(CANDIDATES, dtype=str).fillna("")
    labeled = labeled[labeled.label != ""]
    key = pd.read_csv(KEY, dtype=str)
    s, _ = scored_pairs()
    m = labeled[["pair_id", "LIFNR_A", "LIFNR_B", "label"]].merge(key, on=["pair_id", "LIFNR_A",
                                                                           "LIFNR_B"])
    m = m.merge(s[["LIFNR_A", "LIFNR_B", "score", "baseline_score", "match_weight"]],
                on=["LIFNR_A", "LIFNR_B"])
    metrics = json.loads(Path("data/matching/metrics.json").read_text())
    methods = {"WRatio (chosen)": ("score", metrics["chosen"]["threshold"]),
               "token_set_ratio (baseline)": ("baseline_score",
                                              metrics["baseline_token_set_ratio"]["test"]
                                              ["threshold"]),
               "Splink 5": ("match_weight", metrics["splink"]["threshold_match_weight"])}
    decided = m[m.label.isin(["duplicate", "not_duplicate"])]
    truth = decided.label == "duplicate"
    out = {"labeled": len(m), "label_counts": m.label.value_counts().to_dict(),
           "decided_pairs": len(decided),
           "agreement_with_uei_key": {
               "labeled_duplicate_but_uei_says_different": int(
                   ((m.label == "duplicate") & (m.uei_label != "match")).sum()),
               "labeled_not_duplicate_but_uei_says_same": int(
                   ((m.label == "not_duplicate") & (m.uei_label == "match")).sum())},
           "by_stratum": pd.crosstab(m.stratum, m.label).to_dict(orient="index"),
           "methods": {}}
    for name, (col, t) in methods.items():
        hit = decided[col].fillna(-99) >= t
        tp, fp = int((hit & truth).sum()), int((hit & ~truth).sum())
        fn = int((~hit & truth).sum())
        p = tp / (tp + fp) if tp + fp else 0.0
        r = tp / (tp + fn) if tp + fn else 0.0
        out["methods"][name] = {"threshold": t, "tp": tp, "fp": fp, "fn": fn,
                                "precision": round(p, 3), "recall": round(r, 3),
                                "f1": round(2 * p * r / (p + r), 3) if p + r else 0.0}
    METRICS.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    {"build": build, "score": score}[sys.argv[1]]()
