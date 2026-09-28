"""Bronze: raw SAP tables as extracted, one streaming table per SAP table.

Auto Loader ingests each file exactly once, so landing a date again adds nothing.
Columns stay as delivered (all strings); extract_date comes from the folder name.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

INBOX = spark.conf.get("inbox")  # noqa: F821 - spark is provided by the pipeline runtime
TABLES = ["LFA1", "LFB1", "LFBK", "BUT000", "BUT0BK", "ADRC", "DFKKBPTAXNUM", "CVI_VEND_LINK",
          "ACDOCA", "CDHDR", "CDPOS"]


def bronze_table(table: str) -> None:
    @dp.table(name=f"bronze_{table.lower()}",
              comment=f"Raw {table} rows, one file per extract_date")
    def _load():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.partitionColumns", "")
            .load(f"{INBOX}/{table}/")
            .withColumn("extract_date", F.to_date(F.regexp_extract(
                "_metadata.file_path", r"extract_date=(\d{4}-\d{2}-\d{2})", 1)))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_loaded_at", F.current_timestamp())
        )


for t in TABLES:
    bronze_table(t)
