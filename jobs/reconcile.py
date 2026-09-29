"""Reconcile gold exceptions and silver data quality against the generator manifest.

Synthetic data only: the manifest says which vendors each rule should flag, from which
extract_date, and which decoys must stay clean. Fails if anything does not match.
"""

import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RULES = ["R02", "R03", "R04", "R05", "R06", "R07"]
BY_COMPANY_CODE = {"R05"}
# Background changes that look like R02/R07 activity but must not be flagged.
BASE_DECOYS_FOR = {"R02": {"bank_change", "bank_change_future", "temp_payment_block"},
                   "R07": {"bank_change", "bank_change_future", "temp_payment_block"}}

# variant -> (silver table, condition marking the defect, True if the row should survive)
DQ = {
    "bank_key_present": ("silver_vendor_bank", "BANKN = ''", False),
    "known_doc_type": ("silver_journal_lines", "BLART = 'ZZ'", False),
    "routing_number_9_digits": ("silver_vendor_bank", "length(BANKL) = 8", True),
    "known_payment_terms": ("silver_vendor_company_code", "term_days IS NULL", True),
    "has_business_partner": ("silver_vendor", "PARTNER IS NULL", True),
    "us_address": ("silver_vendor", "COUNTRY != 'US'", True),
}


def expected_first(row, initial: str) -> str:
    if row.rule_id == "R02":
        return re.search(r"revert (\d{8})", row.detail).group(1)  # flagged once complete
    return row.event_date or initial


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    db = f"{a.catalog}.{a.schema}"
    manifest = spark.read.parquet(
        f"/Volumes/{a.catalog}/{a.schema}/landing/staging/_meta/manifest.parquet").toPandas()
    exc = (spark.table(f"{db}.gold_exceptions")
           .select(F.date_format("extract_date", "yyyyMMdd").alias("d"), "rule_id", "LIFNR",
                   "BUKRS").toPandas())
    initial, last = exc.d.min(), exc.d.max()
    ok = True

    print(f"Rules on {last} (vendor level; R05 by vendor and company code)")
    print(f"{'rule':<6}{'injected':>9}{'detected':>9}{'matched':>8}{'missed':>7}{'extra':>6}"
          f"{'decoys hit':>11}{'first day ok':>13}")
    for rule in RULES:
        key = ["LIFNR", "BUKRS"] if rule in BY_COMPANY_CODE else ["LIFNR"]
        m = manifest[manifest.rule_id == rule]
        inj = m[m.expected_flag]
        expected = set(map(tuple, inj[key].values))
        e = exc[exc.rule_id == rule]
        detected = set(map(tuple, e[e.d == last][key].values))
        decoy_ids = set(m[~m.expected_flag].LIFNR)
        base = manifest[(manifest.rule_id == "BASE")
                        & manifest.variant.isin(BASE_DECOYS_FOR.get(rule, set()))]
        decoy_ids |= set(base.LIFNR)
        decoys_hit = len(decoy_ids & set(e.LIFNR))
        first = e.groupby(key).d.min()
        first_ok = sum(first.get(tuple(getattr(r, k) for k in key) if len(key) > 1 else r.LIFNR)
                       == expected_first(r, initial) for r in inj.itertuples())
        missed, extra = len(expected - detected), len(detected - expected)
        ok &= missed == 0 and extra == 0 and decoys_hit == 0 and first_ok == len(inj)
        print(f"{rule:<6}{len(expected):>9}{len(detected):>9}{len(expected & detected):>8}"
              f"{missed:>7}{extra:>6}{decoys_hit:>11}{f'{first_ok}/{len(inj)}':>13}")

    print("\nData quality defects (drop = gone from silver, warn = kept in silver)")
    dq = manifest[manifest.rule_id == "DQ"]
    for variant, (table, cond, survives) in DQ.items():
        ids = sorted(dq[dq.variant == variant].LIFNR)
        found = (spark.table(f"{db}.{table}").where(cond).where(F.col("LIFNR").isin(ids))
                 .select("LIFNR").distinct().count())
        good = found == (len(ids) if survives else 0)
        ok &= good
        policy = "warn" if survives else "drop"
        print(f"  {variant:<26}{policy:<6}planted {len(ids)}, in silver {found}  "
              f"{'OK' if good else 'MISMATCH'}")

    if not ok:
        raise SystemExit("Reconciliation found mismatches")
    print("\nAll rules and data quality checks reconcile with the manifest.")


if __name__ == "__main__":
    main()
