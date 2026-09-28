import pandas as pd
import pytest

from extract.build_vendor_master import (
    build_candidates,
    pair_stats,
    resolve_columns,
    sample_vendors,
)


def tx(rows):
    cols = ["uei", "name_raw", "dba_name", "parent_uei", "parent_name_raw", "address_line_1",
            "city", "state", "zip", "country", "source_file"]
    return pd.DataFrame([dict(zip(cols, r, strict=True)) for r in rows])


ROWS = [
    # Same UEI, two spellings, same address -> 2 vendor rows, 1 positive pair
    ("U1", "Acme Supply Inc", "", "P1", "ACME HOLDINGS", "12 Main Street", "Denver", "CO",
     "80202", "USA", "a.zip"),
    ("U1", "ACME SUPPLY, INC.", "", "P1", "ACME HOLDINGS", "12 Main St", "Denver", "CO",
     "80202", "USA", "a.zip"),
    # Exact repeat of the row above -> collapses into it
    ("U1", "ACME SUPPLY, INC.", "", "P1", "ACME HOLDINGS", "12 MAIN ST.", "Denver", "CO",
     "80202-1111", "USA", "b.zip"),
    # Single-spelling UEI
    ("U2", "Blue River LLC", "", "", "", "9 Elm Ave", "Boise", "ID", "83702", "USA", "a.zip"),
    # Dropped: redacted individual, missing UEI, foreign address
    ("U3", "REDACTED DUE TO PII", "", "", "", "1 A St", "X", "VA", "22000", "USA", "a.zip"),
    ("", "No Uei Corp", "", "", "", "1 B St", "X", "VA", "22000", "USA", "a.zip"),
    ("U4", "Foreign Ltd", "", "", "", "1 C St", "Paris", "", "", "FRA", "a.zip"),
]


def test_build_candidates_dedupes_on_uei_name_address():
    v = build_candidates(tx(ROWS))
    assert sorted(v["uei"].unique()) == ["U1", "U2"]
    assert len(v) == 3
    repeat = v[v["name_raw"] == "ACME SUPPLY, INC."]
    assert repeat["transaction_count"].item() == 2
    assert repeat["address_norm"].item() == "12 MAIN ST"


def test_pair_stats():
    s = pair_stats(build_candidates(tx(ROWS)))
    assert s["distinct_ueis"] == 2
    assert s["ueis_multi_spelling"] == 1
    assert s["positive_pairs_all"] == 1
    assert s["positive_pairs_diff_name"] == 1
    # Both Acme spellings normalize to the same string
    assert s["positive_pairs_diff_norm_name"] == 0
    assert s["ueis_multi_after_normalization"] == 0
    assert s["related_pairs_same_parent"] == 0


def test_related_pairs_counts_different_ueis_under_one_parent():
    rows = [
        ("C1", "Parent East Inc", "", "PX", "PARENT", "1 A St", "Reno", "NV", "89501", "USA", "a"),
        ("C2", "Parent West Inc", "", "PX", "PARENT", "2 B St", "Reno", "NV", "89501", "USA", "a"),
        ("C2", "PARENT WEST INC.", "", "PX", "PARENT", "2 B St", "Reno", "NV", "89501", "USA", "a"),
    ]
    s = pair_stats(build_candidates(tx(rows)))
    # C1 pairs with each of C2's two rows; the C2-C2 pair is a positive, not related.
    assert s["related_pairs_same_parent"] == 2
    assert s["positive_pairs_all"] == 1


def test_vendor_file_never_contains_uei(tmp_path, monkeypatch):
    import extract.build_vendor_master as bvm

    monkeypatch.setattr(bvm, "OUT_VENDORS", tmp_path / "vendors.parquet")
    monkeypatch.setattr(bvm, "OUT_LABELS", tmp_path / "labels" / "labels.parquet")
    bvm.write_outputs(build_candidates(tx(ROWS)))
    vendors = pd.read_parquet(tmp_path / "vendors.parquet")
    labels = pd.read_parquet(tmp_path / "labels" / "labels.parquet")
    assert not {"uei", "parent_uei", "parent_name_raw"} & set(vendors.columns)
    assert list(vendors["vendor_id"]) == list(labels["vendor_id"])


def test_sample_keeps_multi_spelling_ueis_and_is_seeded():
    rows = []
    for i in range(50):
        rows.append((f"S{i}", f"Single {i} Co", "", "", "", f"{i} Oak St", "Reno", "NV",
                     "89501", "USA", "a.zip"))
    for i in range(5):
        for name in (f"Multi {i} Inc", f"MULTI {i} INCORPORATED"):
            rows.append((f"M{i}", name, "", "", "", "1 Pine St", "Reno", "NV", "89501", "USA",
                         "a.zip"))
    v = build_candidates(tx(rows))
    a = sample_vendors(v, target_rows=20, seed=7)
    b = sample_vendors(v, target_rows=20, seed=7)
    assert len(a) == 20
    assert set(a["uei"]) >= {f"M{i}" for i in range(5)}
    pd.testing.assert_frame_equal(a, b)


def test_resolve_columns_prefers_first_candidate_and_checks_required():
    headers = ["recipient_uei", "recipient_name_raw", "recipient_address_line_1",
               "recipient_city_name", "recipient_state_code", "recipient_zip_code",
               "recipient_parent_name"]
    m = resolve_columns(headers)
    assert m["zip"] == "recipient_zip_code"
    assert m["parent_name_raw"] == "recipient_parent_name"
    with pytest.raises(ValueError):
        resolve_columns(["recipient_uei"])
