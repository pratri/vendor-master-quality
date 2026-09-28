"""Export gold (and the silver/SCD2 slices the drill-down needs) as small Parquet files.

The Streamlit demo reads these files, so it runs anywhere without Databricks access.
"""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

EXPORTS = {
    "vendor_risk": "SELECT * FROM {db}.gold_vendor_risk",
    "exceptions": "SELECT * FROM {db}.gold_exceptions",
    "run_history": "SELECT * FROM {db}.gold_run_history",
    "duplicate_candidates": "SELECT * FROM {db}.gold_duplicate_candidates",
    "bank_history": """SELECT LIFNR, BANKS, BANKL, BANKN, source, changed_by, CHANGENR,
                              __START_AT.change_ts AS valid_from, __END_AT.change_ts AS valid_to
                       FROM {db}.dim_vendor_bank_scd2""",
    "controls_history": """SELECT * EXCEPT (__START_AT, __END_AT),
                                  __START_AT.change_ts AS valid_from,
                                  __END_AT.change_ts AS valid_to
                           FROM {db}.dim_vendor_controls_scd2""",
    "open_items": "SELECT * FROM {db}.silver_open_items",
    "vendors": """SELECT * FROM {db}.silver_vendor
                  WHERE extract_date = (SELECT max(extract_date) FROM {db}.silver_vendor)""",
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    db = f"{a.catalog}.{a.schema}"
    out = Path(f"/Volumes/{a.catalog}/{a.schema}/landing/export")
    out.mkdir(parents=True, exist_ok=True)
    for name, sql in EXPORTS.items():
        df = spark.sql(sql.format(db=db))
        # Decimals become doubles: fine for display, simpler for DuckDB and pandas.
        df = df.select([F.col(f.name).cast("double") if isinstance(f.dataType, DecimalType)
                        else F.col(f.name) for f in df.schema.fields])
        pdf = df.toPandas()
        pdf.attrs = {}  # Spark Connect puts plan metrics here; parquet metadata can't hold them
        pdf.to_parquet(out / f"{name}.parquet", index=False)
        print(f"{name}: {len(pdf):,} rows, {(out / f'{name}.parquet').stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
