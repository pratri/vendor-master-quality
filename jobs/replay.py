"""Replay extract dates through the pipeline, or prove that rerunning a date changes nothing.

replay:      for each date, copy its files from staging into the inbox, then run one pipeline
             update. --reset empties the inbox and full-refreshes on the first date.
             --single-update lands every date first and runs one update: same result (silver
             is keyed by extract_date and AUTO CDC orders by sequence), much less overhead.
idempotency: fingerprint every silver/dim/gold table, then
             1. land the date again and run an update (no double counting), and
             2. run a full refresh, rebuilding every table from the raw files (determinism).
             Fingerprint after each and fail if anything differs. Step 1 alone would be weak:
             Auto Loader skips files it has already ingested, so it mostly proves that.
"""

import argparse
import shutil
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLES = ["LFA1", "LFB1", "LFBK", "BUT000", "BUT0BK", "ADRC", "DFKKBPTAXNUM", "CVI_VEND_LINK",
          "ACDOCA", "CDHDR", "CDPOS"]
OUTPUT_PREFIXES = ("silver_", "dim_", "gold_")


def land(staging: Path, inbox: Path, d: str) -> None:
    for t in TABLES:
        src = staging / t / f"extract_date={d}"
        dst = inbox / t / f"extract_date={d}"
        dst.mkdir(parents=True, exist_ok=True)
        for f in src.iterdir():
            shutil.copyfile(f, dst / f.name)


def run_pipeline(w: WorkspaceClient, pipeline_id: str, full_refresh: bool = False) -> None:
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id,
                                         full_refresh=full_refresh).update_id
    while True:
        state = w.pipelines.get_update(pipeline_id=pipeline_id,
                                       update_id=update_id).update.state.value
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(10)
    if state != "COMPLETED":
        raise RuntimeError(f"pipeline update {update_id} ended {state}")


def fingerprint(spark: SparkSession, catalog: str, schema: str) -> dict[str, tuple]:
    """Row count and an order-independent hash per output table (load metadata excluded)."""
    out = {}
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
        if not row.tableName.startswith(OUTPUT_PREFIXES):
            continue
        df = spark.table(f"{catalog}.{schema}.{row.tableName}")
        cols = [c for c in df.columns if not (c.startswith("_") and not c.startswith("__"))]
        r = df.select(F.count("*").alias("n"),
                      F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")).first()
        out[row.tableName] = (r.n, str(r.h))
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["replay", "idempotency"], required=True)
    p.add_argument("--dates", default="all")
    p.add_argument("--reset", default="false")
    p.add_argument("--single-update", default="false")
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--pipeline-id", required=True)
    a = p.parse_args()

    volume = Path(f"/Volumes/{a.catalog}/{a.schema}/landing")
    staging, inbox = volume / "staging", volume / "inbox"
    dates = (sorted(x.name.split("=")[1] for x in (staging / "LFA1").iterdir())
             if a.dates == "all" else a.dates.split(","))
    w = WorkspaceClient()
    reset, single = a.reset.lower() == "true", a.single_update.lower() == "true"

    if a.mode == "replay":
        if reset and inbox.exists():
            shutil.rmtree(inbox)
        t0 = time.time()
        if single:
            for d in dates:
                land(staging, inbox, d)
            run_pipeline(w, a.pipeline_id, full_refresh=reset)
            print(f"{len(dates)} dates in one update: {time.time() - t0:.0f}s", flush=True)
            return
        for i, d in enumerate(dates):
            t0 = time.time()
            land(staging, inbox, d)
            run_pipeline(w, a.pipeline_id, full_refresh=reset and i == 0)
            print(f"{d}: pipeline update completed in {time.time() - t0:.0f}s", flush=True)
        return

    spark = SparkSession.builder.getOrCreate()
    before = fingerprint(spark, a.catalog, a.schema)
    for d in dates:
        land(staging, inbox, d)
        run_pipeline(w, a.pipeline_id)
    rerun = fingerprint(spark, a.catalog, a.schema)
    run_pipeline(w, a.pipeline_id, full_refresh=True)
    rebuilt = fingerprint(spark, a.catalog, a.schema)
    print(f"{'table':<30}{'rows':>10}  {'hash':<24}{'rerun date':>11}{'full rebuild':>13}")
    for t in sorted(before):
        print(f"{t:<30}{before[t][0]:>10}  {before[t][1]:<24}"
              f"{str(before[t] == rerun.get(t)):>11}{str(before[t] == rebuilt.get(t)):>13}")
    if before != rerun or before != rebuilt:
        raise SystemExit("Output changed after rerunning a date or rebuilding from raw files")
    print(f"Rerunning {', '.join(dates)} and a full rebuild from raw files both left all "
          f"{len(before)} output tables identical.")


if __name__ == "__main__":
    main()
