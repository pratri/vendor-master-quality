"""Generate synthetic SAP extracts and optionally upload them to the Unity Catalog volume.

Usage:
    python -m generator --days 10
    python -m generator --days 10 --upload   (to <landing volume>/staging)

Layout (table first, so each bronze table reads one folder):
    data/extracts/<TABLE>/extract_date=YYYY-MM-DD/<TABLE>.parquet
    data/extracts/_meta/manifest.parquet, key_map.parquet, row_counts.csv
"""

import argparse
import json
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from generator.config import Config
from generator.simulate import Simulator

VENDORS = Path("data/vendor_master.parquet")
OUT = Path("data/extracts")


def write(out, root: Path) -> None:
    if root.exists():
        shutil.rmtree(root)
    for d, tables in out.extracts.items():
        for name, df in tables.items():
            path = root / name / f"extract_date={d.isoformat()}"
            path.mkdir(parents=True, exist_ok=True)
            # SAP extracts are character data; an explicit schema keeps empty days typed.
            schema = pa.schema([(c, pa.string()) for c in df.columns])
            pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False),
                           path / f"{name}.parquet")
    meta = root / "_meta"
    meta.mkdir(parents=True, exist_ok=True)
    out.manifest.to_parquet(meta / "manifest.parquet", index=False)
    out.key_map.to_parquet(meta / "key_map.parquet", index=False)
    out.row_counts.to_csv(meta / "row_counts.csv")


def volume_path() -> str:
    """Landing volume from the deployed bundle (dev mode prefixes the schema per user)."""
    summary = subprocess.run(["databricks", "bundle", "summary", "--output", "json"],
                             check=True, capture_output=True, text=True).stdout
    full_name = json.loads(summary)["resources"]["volumes"]["landing"]["id"]
    # The replay job copies one date at a time from staging into the pipeline's inbox.
    return "dbfs:/Volumes/" + full_name.replace(".", "/") + "/staging"


def upload(root: Path) -> str:
    target = volume_path()
    # Replace the whole folder so a rerun leaves no stale partitions behind.
    subprocess.run(["databricks", "fs", "rm", "-r", target], check=False, capture_output=True)
    subprocess.run(["databricks", "fs", "mkdir", target], check=True)
    subprocess.run(["databricks", "fs", "cp", "-r", str(root), target, "--overwrite"],
                   check=True, capture_output=True)
    return target


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()

    cfg = replace(Config(), days=args.days, seed=args.seed)
    out = Simulator(pd.read_parquet(VENDORS), cfg).run()
    write(out, OUT)

    pd.set_option("display.width", 200)
    print("Row counts per table per extract_date:")
    print(out.row_counts.to_string())
    print("\nInjected (expected_flag=True) and decoys (False):")
    print(out.manifest.groupby(["rule_id", "variant", "expected_flag"]).size().to_string())
    if args.upload:
        print(f"\nUploaded to {upload(OUT)}")


if __name__ == "__main__":
    main()
