"""Build the vendor master from USAspending archive zips and run the go/no-go check.

Usage:
    python -m extract.build_vendor_master --agency 014 --target-rows 25000

One vendor row per distinct (UEI, raw name, normalized address). This mimics the same
supplier being created more than once in SAP. The UEI is written only to the hidden
label file, never to the vendor master the matcher reads.
"""

import argparse
import io
import zipfile
from itertools import combinations
from math import comb
from pathlib import Path

import numpy as np
import pandas as pd

from matching.normalize import normalize_address, normalize_name, zip5

RAW_DIR = Path("data/raw")
OUT_VENDORS = Path("data/vendor_master.parquet")
OUT_LABELS = Path("data/labels/vendor_labels.parquet")
SEED = 42

# Our column -> candidate headers in the USAspending files, first match wins.
COLUMN_CANDIDATES = {
    "uei": ["recipient_uei"],
    "name_raw": ["recipient_name_raw"],
    "dba_name": ["recipient_doing_business_as_name"],
    "parent_uei": ["recipient_parent_uei"],
    "parent_name_raw": ["recipient_parent_name_raw", "recipient_parent_name"],
    "address_line_1": ["recipient_address_line_1"],
    "city": ["recipient_city_name"],
    "state": ["recipient_state_code"],
    "zip": ["recipient_zip_4_code", "recipient_zip_code"],
    "country": ["recipient_country_code"],
}
REQUIRED = ["uei", "name_raw", "address_line_1", "city", "state", "zip"]

# Assistance files include individuals with the name redacted or aggregated.
REDACTED_PATTERNS = r"REDACTED|MULTIPLE RECIPIENTS|PRIVATE INDIVIDUAL|INDIVIDUAL RECIPIENT"


def resolve_columns(headers: list[str]) -> dict[str, str]:
    mapping = {}
    for ours, candidates in COLUMN_CANDIDATES.items():
        hit = next((c for c in candidates if c in headers), None)
        if hit:
            mapping[ours] = hit
    missing = [c for c in REQUIRED if c not in mapping]
    if missing:
        raise ValueError(f"required columns not found: {missing}")
    return mapping


def read_archives(agency: str) -> tuple[pd.DataFrame, list[dict]]:
    frames, mappings = [], []
    for zpath in sorted(RAW_DIR.glob(f"FY*_{agency}_*_Full_*.zip")):
        with zipfile.ZipFile(zpath) as zf:
            for member in (m for m in zf.namelist() if m.endswith(".csv")):
                with zf.open(member) as fh:
                    headers = pd.read_csv(fh, nrows=0).columns.tolist()
                mapping = resolve_columns(headers)
                mappings.append({"file": f"{zpath.name}/{member}", **mapping})
                with zf.open(member) as fh:
                    df = pd.read_csv(
                        io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                        usecols=list(mapping.values()),
                        dtype=str,
                        keep_default_na=False,
                    )
                df = df.rename(columns={v: k for k, v in mapping.items()})
                df["source_file"] = zpath.name
                frames.append(df)
                print(f"  read {member}: {len(df):,} transactions", flush=True)
    if not frames:
        raise SystemExit(f"No archive files for agency {agency} in {RAW_DIR}")
    df = pd.concat(frames, ignore_index=True)
    for col in COLUMN_CANDIDATES:
        if col not in df:
            df[col] = ""
    return df, mappings


def build_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    tx = tx.apply(lambda s: s.str.strip() if s.dtype == object else s)
    keep = (
        (tx["uei"] != "")
        & (tx["name_raw"] != "")
        & (tx["address_line_1"] != "")
        & (tx["country"].isin(["USA", ""]))
        & ~tx["name_raw"].str.upper().str.contains(REDACTED_PATTERNS, regex=True)
    )
    tx = tx[keep].copy()
    tx["name_raw"] = tx["name_raw"].str.replace(r"\s+", " ", regex=True)
    tx["address_norm"] = tx["address_line_1"].map(normalize_address)
    tx["zip5"] = tx["zip"].map(zip5)

    key = ["uei", "name_raw", "address_norm"]
    vendors = (
        tx.groupby(key, as_index=False)
        .agg(
            dba_name=("dba_name", "first"),
            parent_uei=("parent_uei", "first"),
            parent_name_raw=("parent_name_raw", "first"),
            address_line_1=("address_line_1", "first"),
            city=("city", "first"),
            state=("state", "first"),
            zip5=("zip5", "first"),
            transaction_count=("source_file", "size"),
        )
        .sort_values(key)
        .reset_index(drop=True)
    )
    vendors["name_norm"] = vendors["name_raw"].map(normalize_name)
    return vendors


def pair_stats(vendors: pd.DataFrame) -> dict:
    per_uei = vendors.groupby("uei").agg(
        rows=("name_raw", "size"), names=("name_raw", "nunique"), norms=("name_norm", "nunique")
    )
    multi = per_uei[per_uei["names"] >= 2]
    return {
        "vendor_rows": len(vendors),
        "distinct_ueis": len(per_uei),
        "ueis_multi_spelling": len(multi),
        # Every two rows sharing a UEI is a positive pair (same supplier created twice).
        "positive_pairs_all": int(sum(comb(n, 2) for n in per_uei["rows"])),
        "positive_pairs_diff_name": int(different_name_pairs(vendors)),
        "ueis_multi_after_normalization": int((per_uei["norms"] >= 2).sum()),
    }


def different_name_pairs(vendors: pd.DataFrame) -> int:
    total = 0
    for _, g in vendors.groupby("uei"):
        if len(g) > 1:
            total += sum(a != b for a, b in combinations(g["name_raw"].tolist(), 2))
    return total


def sample_vendors(vendors: pd.DataFrame, target_rows: int, seed: int = SEED) -> pd.DataFrame:
    """Keep every multi-spelling UEI first, then fill with other vendors at random."""
    rng = np.random.default_rng(seed)
    names_per_uei = vendors.groupby("uei")["name_raw"].transform("nunique")
    multi = vendors[names_per_uei >= 2]
    rest = vendors[names_per_uei < 2]
    if len(multi) > target_rows:
        ueis = rng.permutation(np.asarray(multi["uei"].unique(), dtype=object))
        keep, n = [], 0
        for u in ueis:
            size = int((multi["uei"] == u).sum())
            if n + size > target_rows:
                continue
            keep.append(u)
            n += size
        multi = multi[multi["uei"].isin(keep)]
    fill = target_rows - len(multi)
    rest_ueis = rng.permutation(np.asarray(rest["uei"].unique(), dtype=object))
    rest = rest[rest["uei"].isin(rest_ueis[: max(fill, 0)])]
    rest = rest.groupby("uei", group_keys=False).head(max(fill, 0))
    out = pd.concat([multi, rest]).head(target_rows)
    return out.sort_values(["uei", "name_raw", "address_norm"]).reset_index(drop=True)


def write_outputs(sample: pd.DataFrame) -> None:
    sample = sample.copy()
    sample.insert(0, "vendor_id", [f"V{i:06d}" for i in range(1, len(sample) + 1)])
    vendor_cols = [
        "vendor_id", "name_raw", "name_norm", "dba_name", "address_line_1", "address_norm",
        "city", "state", "zip5", "transaction_count",
    ]
    label_cols = ["vendor_id", "uei", "parent_uei", "parent_name_raw"]
    OUT_VENDORS.parent.mkdir(parents=True, exist_ok=True)
    OUT_LABELS.parent.mkdir(parents=True, exist_ok=True)
    sample[vendor_cols].to_parquet(OUT_VENDORS, index=False)
    sample[label_cols].to_parquet(OUT_LABELS, index=False)
    print(f"Wrote {OUT_VENDORS} ({OUT_VENDORS.stat().st_size / 1e6:.1f} MB) "
          f"and {OUT_LABELS} ({OUT_LABELS.stat().st_size / 1e6:.1f} MB)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--agency", default="014")
    p.add_argument("--target-rows", type=int, default=25000)
    p.add_argument("--examples", type=int, default=20)
    args = p.parse_args()

    tx, mappings = read_archives(args.agency)
    print("\nColumn mapping (ours -> file header):")
    print(pd.DataFrame(mappings).to_string(index=False))

    vendors = build_candidates(tx)
    stats = pair_stats(vendors)
    print(f"\nTransactions read: {len(tx):,}")
    print("\nGo/no-go on the full candidate table:")
    for k, v in stats.items():
        print(f"  {k:<32} {v:>10,}")

    multi_ueis = vendors.groupby("uei")["name_raw"].nunique().loc[lambda s: s >= 2].index
    multi_ueis = np.asarray(multi_ueis, dtype=object)
    rng = np.random.default_rng(SEED)
    picks = rng.choice(multi_ueis, size=min(args.examples, len(multi_ueis)), replace=False)
    print(f"\n{len(picks)} random multi-spelling UEIs:")
    for u in picks:
        names = sorted(vendors.loc[vendors["uei"] == u, "name_raw"].unique())
        print(f"  {u}: " + " | ".join(names))

    sample = sample_vendors(vendors, args.target_rows)
    s_stats = pair_stats(sample)
    print("\nSample:")
    for k, v in s_stats.items():
        print(f"  {k:<32} {v:>10,}")
    write_outputs(sample)


if __name__ == "__main__":
    main()
