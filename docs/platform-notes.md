# Platform notes (Databricks Free Edition)

Checked on 2026-09-28 on a Databricks Free Edition workspace (AWS), Databricks CLI v1.18.0.

| Check | Result |
|---|---|
| Unity Catalog schema + managed volume + upload from local | Pass |
| Asset Bundle deploy of a Lakeflow pipeline with an expectation | Pass (`expect_or_drop` dropped the null row) |
| AUTO CDC with `stored_as_scd_type=2` | Pass (5-row feed gave 5 history rows with `__START_AT` / `__END_AT`) |
| Serverless job with pip dependencies (rapidfuzz, duckdb) | Pass (environment_version 4) |
| AI/BI dashboard deployed from the bundle | Pass (sharing outside the workspace is not available on Free Edition) |

Gotchas:

- The catalog is `workspace`, and there is a single SQL warehouse, "Serverless Starter Warehouse". The bundle looks it up by name.
- `databricks fs cp` does not create folders in a volume. Run `databricks fs mkdir` first.
- Pipeline code uses the current `from pyspark import pipelines as dp` API (`dp.create_auto_cdc_flow`), not the older `import dlt`.
- `sequence_by=F.struct(...)` works for ordering change documents by time and CHANGENR.
- `bundle destroy` also drops the tables a pipeline owns.
- Bundle resource keys must be unique across types (a schema and a pipeline cannot both be `vendor_mdq`).
- Dev mode prefixes the schema per user (`dev_<user>_vendor_mdq`). Read paths from `bundle summary`.
- Spark Connect's `toPandas()` puts plan metrics in `DataFrame.attrs`. Clear them before `to_parquet`.
- PowerShell 5.1 writes UTF-8 with a BOM, which breaks `pyproject.toml`. Edit files with an editor or Python.

## Replay and idempotency

| Run | Result |
|---|---|
| Replay, one pipeline update per date | Every date succeeds. About 2.5 minutes per update, mostly serverless startup. |
| Replay, all dates in one update (`single_update=true`) | Same hashes as the per-date replay |
| `vendor_mdq_idempotency_check`: land a date again and update | Every output table (silver, SCD2, gold) identical, by row count and hash |
| Same job: full refresh, rebuilding every table from raw files | Every output table identical |

Re-landing a date on its own proves little, because Auto Loader skips files it has already ingested.
The full rebuild and the one-update vs. per-date comparison are the real evidence. Output does not
depend on how dates are batched: silver is keyed by extract_date, and AUTO CDC orders by change time
and CHANGENR.
