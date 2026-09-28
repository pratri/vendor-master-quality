# Platform notes (Databricks Free Edition)

Checked on 2026-09-28 against workspace `dbc-771eb7e2-bf8c` (AWS us-east-2), Databricks CLI v1.18.0.

| Check | Result |
|---|---|
| Unity Catalog schema + managed volume + upload from local | Pass |
| Asset Bundle deploy of a Lakeflow pipeline with an expectation | Pass (`expect_or_drop` dropped the null row) |
| AUTO CDC with `stored_as_scd_type=2` | Pass (5-row feed gave 5 history rows with `__START_AT` / `__END_AT`) |
| Serverless job with pip dependencies (rapidfuzz, duckdb) | Pass (environment_version 4) |

Gotchas:

- Catalog is `workspace`. Only one SQL warehouse: "Serverless Starter Warehouse".
- `databricks fs cp` does not create folders in a volume. Run `databricks fs mkdir` first.
- Pipeline code uses the current `from pyspark import pipelines as dp` API (`dp.create_auto_cdc_flow`), not the older `import dlt`.
- `sequence_by=F.struct("UDATE", "UTIME", "CHANGENR")` works for ordering change documents.
- `bundle destroy` also drops the tables a pipeline owns.
