import numpy as np
import pandas as pd

from matching.baseline import combine, score_pairs
from matching.blocking import block_keys, candidate_pairs
from matching.evaluate import fold, label_pairs, positive_pairs, pr_curve
from matching.normalize import normalize_address, normalize_name


def vendors(rows):
    v = pd.DataFrame(rows, columns=["LIFNR", "name", "address", "state", "zip5"])
    v["name_norm"] = v["name"].map(normalize_name)
    v["address_norm"] = v["address"].map(normalize_address)
    return v


V = vendors([
    ("A1", "Acme Supply Inc", "12 Main St", "CO", "80202"),
    ("A2", "ACME SUPPLY, INC.", "12 Main Street", "CO", "80210"),  # same area block
    ("A3", "Acme Supply Incorporated", "9 Elm Ave", "TX", "75001"),  # only name block
    ("B1", "Blue River LLC", "1 Oak Rd", "CO", "80203"),
    ("C1", "Cedar Works", "", "", ""),  # no area key
])


def test_block_keys():
    k = block_keys(V)
    assert list(k.area) == ["CO|802", "CO|802", "TX|750", "CO|802", ""]
    assert list(k.first_token) == ["ACME", "ACME", "ACME", "BLUE", "CEDAR"]


def test_candidate_pairs_union_of_both_passes():
    pairs, stats = candidate_pairs(V)
    got = {(V.LIFNR[a], V.LIFNR[b]) for a, b in zip(pairs.a, pairs.b, strict=True)}
    # area CO|802: A1-A2, A1-B1, A2-B1; first token ACME: A1-A2, A1-A3, A2-A3
    assert got == {("A1", "A2"), ("A1", "B1"), ("A2", "B1"), ("A1", "A3"), ("A2", "A3")}
    assert stats["pairs_area"] == 3 and stats["pairs_first_token"] == 3
    assert stats["pairs_total"] == 5


def test_candidate_pairs_skips_oversized_blocks():
    pairs, stats = candidate_pairs(V, max_block=2)
    assert ("area", "CO|802", 3) in stats["skipped_blocks"]
    assert ("first_token", "ACME", 3) in stats["skipped_blocks"]
    assert len(pairs) == 0


def test_scores_rank_true_duplicates_higher():
    pairs, _ = candidate_pairs(V)
    s = score_pairs(V, pairs)
    s["score"] = combine(s, "WRatio", 0.9)
    s["key"] = [V.LIFNR[a] + V.LIFNR[b] for a, b in zip(s.a, s.b, strict=True)]
    by = s.set_index("key").score
    assert by["A1A2"] > 95 and by["A1A3"] > 90
    assert by["A1B1"] < 60


def test_wratio_penalizes_word_subsets_more_than_token_set():
    v = vendors([("X", "Red Inc", "1 A St", "CO", "80202"),
                 ("Y", "Red Sky Inc", "2 B St", "CO", "80202")])
    s = score_pairs(v, pd.DataFrame({"a": [0], "b": [1]}))
    assert s.name_token_set_ratio[0] == 100
    assert s.name_WRatio[0] < 100


LABELS = pd.DataFrame({
    "LIFNR": ["A1", "A2", "A3", "B1", "B2", "C1"],
    "uei": ["U1", "U1", "U1", "U2", "U3", "U4"],
    "parent_uei": ["", "", "", "P", "P", "U1"],
})


def test_positive_pairs_are_all_pairs_within_a_uei():
    pos = positive_pairs(LABELS)
    assert set(zip(pos.LIFNR_A, pos.LIFNR_B, strict=True)) == {
        ("A1", "A2"), ("A1", "A3"), ("A2", "A3")}


def test_label_pairs_match_related_non_match():
    pairs = pd.DataFrame({"LIFNR_A": ["A1", "B1", "A1", "A2"],
                          "LIFNR_B": ["A2", "B2", "C1", "B1"]})
    # same UEI; same parent; C1's parent is A1's UEI; unrelated
    assert list(label_pairs(pairs, LABELS)) == ["match", "related", "related", "non_match"]


def test_pr_curve_counts_blocking_misses_against_recall():
    score = np.array([95, 80, 99, 50])
    label = np.array(["match", "match", "non_match", "related"])
    c = pr_curve(score, label, n_positive=4, thresholds=[90])
    r = c.iloc[0]
    assert (r.tp, r.fp) == (1, 1)  # related pairs are neither
    assert r.precision == 0.5 and r.recall == 0.25


def test_fold_is_deterministic_and_balanced():
    p = pd.DataFrame({"LIFNR_A": [f"{i:06d}" for i in range(2000)], "LIFNR_B": "999999"})
    f1, f2 = fold(p), fold(p)
    assert (f1 == f2).all()
    assert 0.45 < f1.mean() < 0.55
