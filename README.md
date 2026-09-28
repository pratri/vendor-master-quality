# Vendor master data quality pipeline

Duplicate vendors and last-minute bank changes cost AP teams money. This pipeline does exception
monitoring and control testing over SAP vendor master data on Databricks.

| Vendor records processed | Duplicate matcher precision / recall (held-out, threshold 91) | Open exposure flagged |
|---|---|---|
| **20,150** | **0.78 / 0.86** | **$41.2M** (on the last extract date) |

![Streamlit demo: ranked vendor risk](docs/app_screenshot.png)

**Live demo:** [vendor-master-quality.streamlit.app](https://vendor-master-quality-afvmiyhfyy4yhg32upadql.streamlit.app/) ·
**Evaluation:** [docs/evaluation.md](docs/evaluation.md)

**Architecture:** extract replay → bronze → silver → SCD2 change history → rules and matcher → gold → Streamlit demo

> Vendor names and addresses are real public data from USAspending.gov. All transactional data is synthetic, shaped to SAP DDIC table and field names, and was not extracted from a live system.

### Run the demo (committed data, no Databricks needed)

```powershell
py -3.11 -m venv .venv; .\.venv\Scripts\pip install -e ".[dev,app]"
.\.venv\Scripts\streamlit run app/streamlit_app.py
```

### Rebuild everything on Databricks (Free Edition works)

```powershell
databricks auth login --host <your-workspace-url>
databricks bundle deploy
python -m generator --days 10 --upload        # seeded SAP extracts -> landing volume
python -m matching --upload                   # duplicate candidates -> landing volume
databricks bundle run replay --params "single_update=true"
databricks bundle run reconcile               # rules vs. the generator's manifest
databricks bundle run idempotency_check       # rerun one date, compare every output table
databricks bundle run export_demo             # gold -> Parquet for the demo
```

---

## What it checks

| Rule | Checks | SAP fields read | Severity |
|---|---|---|---|
| R01 Duplicate vendor | Matcher pair above threshold, both vendors active with open items or a payment in the last 90 days | matcher output; LFA1-LOEVM, LFA1-SPERR | Medium |
| R02 Change, pay, revert | A bank change, a clearing payment, then a second bank change within 7 days | CDHDR/CDPOS on LFBK; ACDOCA BLART KZ | High |
| R03 Shared bank account | Same BANKS + BANKL + BANKN on more than one vendor; High when one holder is an employee vendor | LFBK; LFB1-PERNR | Medium / High |
| R04 Dormant but not blocked | No postings for 18 months and none of SPERR, SPERZ, LOEVM set (SPERM alone does not count) | ACDOCA; LFA1-SPERR, SPERZ, LOEVM | Low |
| R05 Duplicate invoice check off | LFB1-REPRF blank | LFB1-REPRF | Medium |
| R07 Unconfirmed change, items due | CONFS set and open items due by extract date + 7 days | LFA1-CONFS, LFB1-CONFS; ACDOCA NETDT, AUGBL | High |

R06 (alternative payee and one-time vendors) is defined but deferred.

**Risk score.** `exposure` = sum of absolute open item amounts (ACDOCA HSL, AUGBL blank) for the vendor.
`risk` = exposure × the highest severity tier among its exceptions (High 3, Medium 2, Low 1). No other
weights. Vendors are ranked by risk per extract date. Two consequences of this method: R01 dominates the
default ranking, because the dataset contains about 2,900 real duplicate pairs, and dormant vendors
(R04) have no exposure, so their risk is 0. The demo therefore filters by rule and severity.

## Architecture

```
generator (local, seeded) --> landing volume: staging/  --replay job-->  inbox/
                                                                           |
            Lakeflow Declarative Pipeline (serverless, deployed by the bundle)   v
  bronze_* (11 streaming tables, Auto Loader)
    -> silver_* (8 materialized views, 28 expectations)
    -> dim_vendor_bank_scd2, dim_vendor_controls_scd2 (AUTO CDC, SCD type 2)
    -> rule_r01..r07 views -> gold_exceptions, gold_vendor_risk, gold_run_history,
       gold_duplicate_candidates (from the rapidfuzz matcher)
jobs: replay, reconcile, idempotency_check, export_demo  -->  Streamlit (DuckDB over Parquet)
```

Everything in the workspace (schema, volume, pipeline, jobs) is defined in `databricks.yml`.

**SAP model.** The target is SAP ECC 6.0 and S/4HANA (on-premise and private cloud). This README says
"vendor" throughout; S/4HANA calls it "supplier". In S/4HANA the Business Partner is where data is
maintained, and CVI keeps the classic vendor tables in step, so both sides are modeled:

- **Vendor side:** LFA1, LFB1, LFBK.
- **Business Partner side:** BUT000, BUT0BK (with validity dates), DFKKBPTAXNUM.
- **Link and address:** CVI_VEND_LINK, ADRC.
- **Journal:** ACDOCA.
- **Change documents:** CDHDR/CDPOS (object class KRED).

## Design decisions

- **Idempotency in three layers.**
  1. Auto Loader ingests each file exactly once.
  2. Silver and gold are materialized views, pure functions of bronze.
  3. AUTO CDC orders changes by change time and CHANGENR, not arrival order.

  **Proof:** rerunning 2026-09-05 leaves all 14 output tables identical (row count and hash). Replaying
  10 dates as 10 updates or as one gives the same hashes, in 26 minutes vs. 3.
- **Change history from change documents, not snapshots.** A bank change, payment and revert inside one
  day leaves the end-of-day snapshot unchanged. CDPOS shows all of it: for vendor 0000101379 a clerk
  switched the bank at 08:15 on 2026-09-04, the noon payment run paid $7,092.78, and the bank was
  switched back at 16:45. In LFBK a bank account change is a delete plus an insert, because the account number is part
  of the key, so the insert's TABKEY carries the new bank.
- **Bank validity.** A BUT0BK row entered with a future BK_VALID_FROM does not reach LFBK until it becomes
  valid, so bank history never applies it early. 26 such rows sit past the window and none appear
  in the history.
- **Ledger filters matter.** Open items are ACDOCA lines with `RLDNR = '0L'`, `KOART = 'K'` and AUGBL
  blank. Without those filters the open exposure comes out about 40 times too high (two ledgers plus
  offset lines).
- **ACDOCA arrives as a daily delta.** Each day brings that day's lines plus earlier lines cleared that
  day, re-sent with AUGBL filled. Silver keeps the latest version and when it was first cleared, so
  open items can be rebuilt as of any extract date.
- **Expectations follow a written policy.**
  - **Fail** on broken keys.
  - **Drop** rows no rule can use.
  - **Warn** on suspicious values that are still usable.

  The generator plants six kinds of defects so this is visible: 2 dropped, 4 warned, and all
  reconciled.
- **Every rule is reconciled against ground truth.** The generator injects control exceptions and look-alike
  decoys at known rates and writes a manifest. On the last date, all five injected rules match exactly:
  - no misses and no extras
  - no decoys flagged
  - every vendor first flagged on the expected day.

## Duplicate matching

- **Blocking:** two passes (state + ZIP3, then first name token) keep 99.4% of true pairs in 895,000
  candidates, out of 203 million possible.
- **Scoring:** a rapidfuzz score, 0.9 × name (WRatio) + 0.1 × address (token_set_ratio), tuned on half
  the pairs and reported on the other half.
- **Results:** precision 0.78, recall 0.86, F1 0.82. The `token_set_ratio` baseline scores
  0.69 / 0.88 / 0.78.
- **Label noise:** 92% of false positives are identical names under different UEIs (companies with
  several registrations), so strict precision is a floor.

Details, the precision-recall curve and the error analysis are in [docs/evaluation.md](docs/evaluation.md).

## Context

Related tools: SAP MDG, Marlin Duplicate Check, Trustpair, Eftsure, apexanalytix.

This complements SAP's own controls rather than standing in for them:

- sensitive fields with dual control (T055F, confirmation with FK08/FK09 and the CONFS status)
- change documents
- the vendor change display.

**S/4HANA Public Cloud** does not allow table access, so a version for it would read released CDS views
such as I_Supplier and I_SupplierCompany.

## Limitations

- All transactional data is synthetic. The only evaluation claim is for duplicate matching on real names.
- Auto Loader ingests each file once. A corrected re-delivery of a day needs a full refresh (3 minutes
  here).
- The UEI answer key has label noise (see above). Hand-labeling the borderline pairs is deferred.
- Deferred: Splink, R06, CI, the Databricks AI/BI dashboard, a 30-day replay.
- **SAP modeling assumptions**, each marked in the code:
  - BP change documents use object class BUPA_BUP.
  - ACDOCA offset lines also carry LIFNR.
  - EIN goes in STCD2 and BP tax type US2.
  - CVI moves a time-dependent BP bank row to LFBK when it becomes valid.
  - Payment term keys (NT30, NT45, NT60) are typical customer configuration, not SAP-delivered.
- Runs on Databricks Free Edition (AWS). Nothing here is AWS-specific.

## What I would change with real system access

- Read CDHDR/CDPOS directly instead of synthesizing change documents.
- Pull F110 payment run data (REGUH/REGUP), so R02 can use the actual paid-to bank and the payment time.
- Respect bank validity dates as delivered by the source system (BUT0BK), including rows changed after
  the fact.

## Repo layout

```
extract/     USAspending fetch and vendor master build
generator/   seeded synthetic SAP tables, daily extract replay, manifest
pipelines/   Lakeflow pipeline: bronze, silver, SCD2, gold (+ pipeline.yml)
matching/    normalization, blocking, rapidfuzz scoring, evaluation
jobs/        replay, reconcile, idempotency check, demo export (+ job YAML)
app/         Streamlit demo and its exported Parquet data
docs/        evaluation report, platform notes, charts
tests/       pytest (generator, matching, normalization, bundle config)
```

## Deploy the demo

The app reads only `app/data/`, so Streamlit Community Cloud can host it straight from this repo:
New app → this repository, branch `master` → main file `app/streamlit_app.py`. Dependencies come
from `app/requirements.txt`.
