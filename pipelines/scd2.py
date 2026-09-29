"""SCD2 change history built from CDPOS with AUTO CDC.

Day 1 snapshot is the baseline. Later versions come from change documents ordered by change
time and CHANGENR, so a same-day change and revert shows up as two versions.
Bank history uses LFBK changes: CVI only moves a future-dated BUT0BK row once it's valid.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INITIAL = spark.conf.get("initial_load_date")  # noqa: F821
CONTROLS = {"LFA1": ["SPERR", "SPERZ", "SPERM", "LOEVM", "CONFS"],
            "LFB1": ["SPERR", "LOEVM", "ZAHLS", "REPRF", "CONFS"]}
CONTROL_COLS = [f"{t}_{f}" for t, fs in CONTROLS.items() for f in fs]

# CDPOS TABKEY is the client (3) followed by the table key fields at fixed widths.
LIFNR_KEY = F.substring("TABKEY", 4, 10)


def initial_rows(table: str) -> DataFrame:
    return (spark.readStream.table(table)  # noqa: F821
            .where(F.col("extract_date") == F.lit(INITIAL).cast("date")))


def change_docs(tabnames: list[str]) -> DataFrame:
    """Vendor (KRED) change document items with their header timestamp."""
    hdr = (spark.read.table("bronze_cdhdr")  # noqa: F821
           .select("OBJECTCLAS", "OBJECTID", "CHANGENR", "USERNAME", "UDATE", "UTIME")
           .dropDuplicates(["OBJECTCLAS", "OBJECTID", "CHANGENR"]))
    pos = (spark.readStream.table("bronze_cdpos")  # noqa: F821
           .where((F.col("OBJECTCLAS") == "KRED") & F.col("TABNAME").isin(tabnames)))
    return (pos.join(hdr, ["OBJECTCLAS", "OBJECTID", "CHANGENR"])
            .withColumn("change_ts", F.to_timestamp(F.concat("UDATE", "UTIME"),
                                                    "yyyyMMddHHmmss")))


@dp.view(comment="Bank changes per vendor: initial LFBK rows plus LFBK inserts from CDPOS")
def bank_change_feed():
    initial = initial_rows("bronze_lfbk").select(
        "LIFNR", "BANKS", "BANKL", "BANKN",
        F.to_timestamp(F.lit(INITIAL)).alias("change_ts"),
        F.lit("0000000000").alias("CHANGENR"),
        F.lit("initial_load").alias("source"), F.lit(None).cast("string").alias("changed_by"))
    # LFBK bank change = delete + insert, new account is in the insert's TABKEY
    changes = change_docs(["LFBK"]).where(F.col("CHNGIND") == "I").select(
        LIFNR_KEY.alias("LIFNR"),
        F.trim(F.substring("TABKEY", 14, 3)).alias("BANKS"),
        F.trim(F.substring("TABKEY", 17, 15)).alias("BANKL"),
        F.trim(F.substring("TABKEY", 32, 18)).alias("BANKN"),
        "change_ts", "CHANGENR", F.lit("CDPOS").alias("source"),
        F.col("USERNAME").alias("changed_by"))
    return initial.unionByName(changes)


dp.create_streaming_table(
    "dim_vendor_bank_scd2",
    comment="Vendor bank account history (SCD type 2) from change documents")
dp.create_auto_cdc_flow(
    target="dim_vendor_bank_scd2",
    source="bank_change_feed",
    keys=["LIFNR"],
    sequence_by=F.struct("change_ts", "CHANGENR"),
    stored_as_scd_type=2,
    track_history_column_list=["BANKS", "BANKL", "BANKN"],
)


@dp.view(comment="Control field changes per vendor and company code. LFA1 changes apply to "
                 "every company code of the vendor; unchanged fields are null")
def controls_change_feed():
    lfa1 = (spark.read.table("bronze_lfa1")  # noqa: F821
            .where(F.col("extract_date") == F.lit(INITIAL).cast("date"))
            .select("LIFNR", *[F.col(f).alias(f"LFA1_{f}") for f in CONTROLS["LFA1"]]))
    meta = [F.to_timestamp(F.lit(INITIAL)).alias("change_ts"),
            F.lit("0000000000").alias("CHANGENR"), F.lit("").alias("TABNAME"),
            F.lit("").alias("FNAME"), F.lit("initial_load").alias("source"),
            F.lit(None).cast("string").alias("changed_by")]
    initial = (initial_rows("bronze_lfb1").join(lfa1, "LIFNR")
               .select("LIFNR", "BUKRS", *[F.col(f).alias(f"LFB1_{f}")
                                          for f in CONTROLS["LFB1"]],
                       *[F.col(c) for c in CONTROL_COLS if c.startswith("LFA1_")], *meta))

    fields = [f"{t}.{f}" for t, fs in CONTROLS.items() for f in fs]
    cd = (change_docs(["LFA1", "LFB1"])
          .where((F.col("CHNGIND") == "U")
                 & F.concat_ws(".", "TABNAME", "FNAME").isin(fields))
          .withColumn("LIFNR", LIFNR_KEY))
    company_codes = spark.read.table("bronze_lfb1").select("LIFNR", "BUKRS").distinct()  # noqa: F821
    lfb1_changes = (cd.where(F.col("TABNAME") == "LFB1")
                    .withColumn("BUKRS", F.substring("TABKEY", 14, 4)))
    lfa1_changes = cd.where(F.col("TABNAME") == "LFA1").join(company_codes, "LIFNR")
    changes = lfb1_changes.unionByName(lfa1_changes).select(
        "LIFNR", "BUKRS",
        *[F.when((F.col("TABNAME") == t) & (F.col("FNAME") == f), F.col("VALUE_NEW"))
          .alias(f"{t}_{f}") for t, fs in CONTROLS.items() for f in fs],
        "change_ts", "CHANGENR", "TABNAME", "FNAME", F.lit("CDPOS").alias("source"),
        F.col("USERNAME").alias("changed_by"))
    return initial.select(changes.columns).unionByName(changes)


dp.create_streaming_table(
    "dim_vendor_controls_scd2",
    comment="Vendor control field history (blocks, CONFS, REPRF, ZAHLS) per company code, "
            "SCD type 2")
dp.create_auto_cdc_flow(
    target="dim_vendor_controls_scd2",
    source="controls_change_feed",
    keys=["LIFNR", "BUKRS"],
    sequence_by=F.struct("change_ts", "CHANGENR", "TABNAME", "FNAME"),
    stored_as_scd_type=2,
    ignore_null_updates=True,
    track_history_column_list=CONTROL_COLS,
)
