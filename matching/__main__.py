"""Run the duplicate vendor matcher on the SAP vendor extract and evaluate it.

Usage:
    python -m matching            (writes data/matching/ and docs/pr_curve.png)
    python -m matching --upload   (also sends duplicate_candidates to the landing volume)

Input is the initial load snapshot: ADRC name and address, LFA1 account group (employee
vendors are excluded). The matcher never sees the UEI; labels are joined only to evaluate.
The name weight and threshold are tuned on half the pairs (dev) and reported on the other
half (test).
"""

import argparse
import json
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from generator.config import Config
from matching.baseline import NAME_SCORERS, combine, score_pairs
from matching.blocking import candidate_pairs
from matching.evaluate import fold, label_pairs, positive_pairs, pr_curve
from matching.normalize import normalize_address, normalize_name

EXTRACTS = Path("data/extracts")
LABELS = Path("data/labels/vendor_labels.parquet")
OUT = Path("data/matching")
CHART = Path("docs/pr_curve.png")
NAME_WEIGHTS = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
NEAR_MISS = 10  # also keep pairs this far below the threshold, for review


def load_vendors() -> pd.DataFrame:
    part = f"extract_date={Config().start.isoformat()}"
    lfa1 = pd.read_parquet(EXTRACTS / "LFA1" / part / "LFA1.parquet")
    adrc = pd.read_parquet(EXTRACTS / "ADRC" / part / "ADRC.parquet")
    v = lfa1.loc[lfa1.KTOKK != "ZEMP", ["LIFNR", "ADRNR"]].merge(
        adrc, left_on="ADRNR", right_on="ADDRNUMBER")
    address = np.where(v.STREET != "", v.STREET, "PO BOX " + v.PO_BOX)
    v = pd.DataFrame({"LIFNR": v.LIFNR, "name": v.NAME1, "address": address, "city": v.CITY1,
                      "state": v.REGION, "zip5": v.POST_CODE1})
    v["name_norm"] = v["name"].map(normalize_name)
    v["address_norm"] = v["address"].map(normalize_address)
    return v.sort_values("LIFNR").reset_index(drop=True)


def load_labels(v: pd.DataFrame) -> pd.DataFrame:
    keys = pd.read_parquet(EXTRACTS / "_meta" / "key_map.parquet")
    labels = pd.read_parquet(LABELS).merge(keys, on="vendor_id")
    return labels[labels.LIFNR.isin(v.LIFNR)][["LIFNR", "uei", "parent_uei"]]


def metrics_at(curve: pd.DataFrame, t: int) -> dict:
    r = curve[curve.threshold == t].iloc[0]
    return {k: (round(float(r[k]), 4) if k in ("precision", "recall", "f1") else int(r[k]))
            for k in ("threshold", "tp", "fp", "precision", "recall", "f1")}


def tune(scored: pd.DataFrame, dev: np.ndarray, n_pos: int,
         scorers: list[str]) -> tuple[str, float, int]:
    """Best (name scorer, name weight, threshold) by F1 on the dev fold only."""
    best = (-1.0, scorers[0], 1.0, 100)
    for s in scorers:
        for w in NAME_WEIGHTS:
            c = pr_curve(combine(scored, s, w)[dev], scored.label.to_numpy()[dev], n_pos)
            r = c.loc[c.f1.idxmax()]
            if r.f1 > best[0]:
                best = (r.f1, s, w, int(r.threshold))
    return best[1], best[2], best[3]


def plot(curves: dict[str, pd.DataFrame], chosen: dict[str, int]) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = ["#2a78d6", "#eb6834"]  # categorical slots 1 and 2, validated for CVD
    ink, muted, grid, axis, surface = "#0b0b0b", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
    fig, ax = plt.subplots(figsize=(7.2, 4.6), dpi=150)
    fig.patch.set_facecolor(surface)
    ax.set_facecolor(surface)
    for (name, c), color in zip(curves.items(), colors, strict=True):
        ax.plot(c.recall, c.precision, color=color, linewidth=2, label=name)
        p = c[c.threshold == chosen[name]].iloc[0]
        ax.plot(p.recall, p.precision, "o", markersize=8, color=color,
                markeredgecolor=surface, markeredgewidth=2)
        first = color == colors[0]  # chosen above-left of its point, baseline below-right
        ax.annotate(f"{name}\nthreshold {chosen[name]}", (p.recall, p.precision),
                    xytext=(-12, 12) if first else (12, -30), textcoords="offset points",
                    ha="right" if first else "left", fontsize=8, color=ink)
    ax.set_xlabel("Recall (share of all true duplicate pairs found)", color=muted, fontsize=9)
    ax.set_ylabel("Precision", color=muted, fontsize=9)
    ax.set_title("Duplicate vendor matcher, held-out test pairs", color=ink, fontsize=11,
                 loc="left")
    ax.grid(color=grid, linewidth=0.8)
    ax.tick_params(colors=muted, labelsize=8)
    for s in ax.spines.values():
        s.set_color(axis)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, fontsize=8, labelcolor=ink, loc="lower left")
    fig.tight_layout()
    CHART.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(CHART, facecolor=surface)


def upload(path: Path) -> str:
    from generator.volume import landing_volume

    target = landing_volume() + "/matcher"
    subprocess.run(["databricks", "fs", "mkdir", target], check=True)
    subprocess.run(["databricks", "fs", "cp", str(path), f"{target}/{path.name}", "--overwrite"],
                   check=True, capture_output=True)
    return target


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()

    v = load_vendors()
    pairs, stats = candidate_pairs(v)
    scored = score_pairs(v, pairs)
    scored["LIFNR_A"] = v.LIFNR.to_numpy()[scored.a]
    scored["LIFNR_B"] = v.LIFNR.to_numpy()[scored.b]
    labels = load_labels(v)
    pos = positive_pairs(labels)
    scored["label"] = label_pairs(scored[["LIFNR_A", "LIFNR_B"]], labels).to_numpy()
    scored["fold"], pos["fold"] = fold(scored), fold(pos)
    dev, test = scored.fold.to_numpy() == 0, scored.fold.to_numpy() == 1
    n_dev, n_test = int((pos.fold == 0).sum()), int((pos.fold == 1).sum())

    scorer, w, t = tune(scored, dev, n_dev, list(NAME_SCORERS))
    _, w_base, t_base = tune(scored, dev, n_dev, ["token_set_ratio"])
    score = combine(scored, scorer, w)
    base = combine(scored, "token_set_ratio", w_base)
    lab = scored.label.to_numpy()
    main_label, base_label = f"{scorer} (chosen)", "token_set_ratio (baseline)"
    curves = {main_label: pr_curve(score[test], lab[test], n_test),
              base_label: pr_curve(base[test], lab[test], n_test)}
    chosen = {main_label: t, base_label: t_base}

    found = int((lab == "match").sum())
    related = lab == "related"
    names = v.name_norm.to_numpy()
    fp_mask = test & (lab == "non_match") & (score >= t)
    same_name_fp = int((names[scored.a.to_numpy()] == names[scored.b.to_numpy()])[fp_mask].sum())
    metrics = {
        "vendors": len(v), "positive_pairs": len(pos),
        "blocking": {**{k: val for k, val in stats.items() if k != "skipped_blocks"},
                     "skipped_blocks": [list(x) for x in stats["skipped_blocks"]],
                     "positives_in_candidates": found,
                     "blocking_recall": round(found / len(pos), 4)},
        "labels_in_candidates": scored.label.value_counts().to_dict(),
        "chosen": {"name_scorer": scorer, "name_weight": w, "threshold": t,
                   "tuned_on": "dev fold"},
        "test": metrics_at(curves[main_label], t),
        "dev": metrics_at(pr_curve(score[dev], lab[dev], n_dev), t),
        "baseline_token_set_ratio": {"name_weight": w_base,
                                     "test": metrics_at(curves[base_label], t_base)},
        "test_false_positives_same_name_different_uei": same_name_fp,
        "related_pairs": {"in_candidates": int(related.sum()),
                          "above_threshold": int((related & (score >= t)).sum())},
    }

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2))
    pd.concat([c.assign(method=k) for k, c in curves.items()]).to_csv(OUT / "pr_curve.csv",
                                                                     index=False)
    plot(curves, chosen)

    # Error samples from the test fold, with names and addresses for the write-up.
    info = v.set_index("LIFNR")[["name", "address", "city", "state", "zip5"]]

    def describe(df: pd.DataFrame) -> pd.DataFrame:
        return (df.join(info.add_suffix("_A"), on="LIFNR_A")
                .join(info.add_suffix("_B"), on="LIFNR_B"))

    scored["score"] = score
    scored["name_score"] = scored[f"name_{scorer}"]
    tst = scored[test]
    fp = tst[(tst.label == "non_match") & (tst.score >= t)].sort_values(
        ["score", "LIFNR_A"], ascending=[False, True]).head(10)
    fn_scored = tst[(tst.label == "match") & (tst.score < t)].sort_values(
        ["score", "LIFNR_A"]).head(10)
    seen = set(zip(scored.LIFNR_A, scored.LIFNR_B, strict=True))
    not_blocked = np.array([(a, b) not in seen for a, b in
                            zip(pos.LIFNR_A, pos.LIFNR_B, strict=True)])
    missed = pos[(pos.fold == 1).to_numpy() & not_blocked].head(5)
    cols = ["LIFNR_A", "LIFNR_B", "name_score", "address_score", "score"]
    describe(fp[cols]).to_csv(OUT / "errors_false_positives.csv", index=False)
    describe(pd.concat([fn_scored[cols], missed.assign(name_score=np.nan)[["LIFNR_A",
                        "LIFNR_B", "name_score"]]])).to_csv(OUT / "errors_false_negatives.csv",
                                                             index=False)

    cand = scored[scored.score >= t - NEAR_MISS][
        ["LIFNR_A", "LIFNR_B", "name_score", "address_score", "score"]].copy()
    cand["score"] = cand["score"].round(2)
    cand["above_threshold"] = cand["score"] >= t
    cand["threshold"], cand["name_weight"], cand["name_scorer"] = t, w, scorer
    cand_path = OUT / "duplicate_candidates.parquet"
    cand.sort_values(["LIFNR_A", "LIFNR_B"]).to_parquet(cand_path, index=False)

    print(json.dumps(metrics, indent=2))
    print(f"\n{len(cand):,} candidate pairs written ({int(cand.above_threshold.sum()):,} "
          f"at or above threshold {t})")
    if args.upload:
        print(f"Uploaded to {upload(cand_path)}")


if __name__ == "__main__":
    main()
