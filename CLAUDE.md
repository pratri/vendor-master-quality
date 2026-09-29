# Vendor Master Data Quality Pipeline

## What this project is

A data engineering portfolio project. It ingests SAP-shaped vendor master data, tracks changes to sensitive fields over time, runs control-testing rules, finds duplicate vendors with entity resolution, and ranks exceptions by open payables exposure.

Audience: people watching a demo, mainly hiring managers for Data Engineer roles on Azure and Databricks. Every design choice should be something I can defend in an interview.

Goal right now: a working demo prototype, fast. Prefer the simplest version that runs end to end. Anything listed under "Deferred" below is out of scope until the core demo works.

I am the author and I am learning Databricks through this build. When you create or change a file, explain in plain terms what it does and why it is built that way. Keep explanations short and concrete.

## Stack

1. Databricks Free Edition (serverless). PySpark, Delta Lake, Unity Catalog.
2. Lakeflow Declarative Pipelines (formerly Delta Live Tables): expectations for data quality, AUTO CDC (formerly APPLY CHANGES) with SCD type 2 for change history. If AUTO CDC fights us on Free Edition, fall back to a plain Delta MERGE for SCD2 and tell me.
3. Databricks Jobs for orchestration. Databricks Asset Bundles (`databricks.yml`) so the whole workspace setup is defined in the repo.
4. Local Python 3.11 on Windows for data extraction and synthetic data generation. Use PowerShell syntax for any commands you give me.
5. Entity resolution: rapidfuzz (production matcher) plus Splink 5.0.0 with the DuckDB backend as an evaluated comparison.
6. pytest and ruff, run by GitHub Actions CI; `databricks bundle validate` runs when the DATABRICKS_HOST and DATABRICKS_TOKEN secrets are set.
7. Demo surface: a small Streamlit app over exported gold Parquet files. This is the main demo, not optional. An AI/BI dashboard is also deployed by the bundle for workspace users.

Databricks Free Edition has limits and its feature set changes. Before relying on a feature (pipelines, AUTO CDC, jobs, bundles, dashboards, pip installs on serverless), verify that it works in my workspace. If something is unavailable, stop and tell me, and propose the closest fallback.

## Data sources

### Real data: vendor names and addresses (USAspending.gov)

USAspending award transaction download files. No API key is needed. Use the Award Data Archive or the bulk download API.

1. Scope: one mid-sized federal agency, one or two recent fiscal years, contracts plus assistance. This avoids the multi-GB Department of Defense files.
2. Columns to keep: `recipient_uei`, `recipient_name_raw`, `recipient_doing_business_as_name`, `recipient_parent_uei`, `recipient_parent_name_raw`, `recipient_address_line_1`, city, state, zip. Check the actual headers in the downloaded files before assuming these names.
3. Use `recipient_name_raw`. The normalized `recipient_name` is identical within a UEI and is useless for matching.
4. Commit only a fetch script and a derived Parquet file under 50 MB. Never commit raw zips. Add them to `.gitignore`.

### Why this source

The same UEI (the government's unique entity ID) appears under different raw name spellings. Each distinct (UEI, raw name, address) combination becomes a separate vendor record, which simulates the same supplier being created twice in SAP. The UEI then works as a hidden answer key for evaluating the duplicate matcher. The matcher must never see the UEI.

`recipient_parent_uei` gives a third class: related entities that are distinct companies. Keep those out of the negative pairs and report them separately.

### Synthetic data

Everything transactional is synthetic and seeded for reproducibility: company codes, company-code vendor data, bank details, tax numbers, invoices, payments and change documents. Bank account and tax numbers must be obviously fake. Control exceptions (bank changes, blocks, dormant vendors) are injected by the generator at known rates. The README must disclose this plainly. The only evaluation claim the project makes is for duplicate matching, which runs on real names.

## SAP data model

Target framing: SAP ECC 6.0 and S/4HANA (on-premise and private cloud). Use "vendor" throughout and mention once that S/4HANA calls it "supplier". In S/4HANA the Business Partner is the maintenance entry point, and CVI (customer vendor integration) keeps the classic vendor tables populated. Model both sides.

Use real SAP DDIC table and field names. If you are unsure whether a field exists, say so. Do not invent field names.

| Table | Purpose | Key fields |
|---|---|---|
| LFA1 | Vendor general data | LIFNR, KTOKK, NAME1, ADRNR, STCD1, STCD2, SPERR (posting block), SPERZ (payment block), SPERM (purchasing block), LOEVM (deletion flag), XCPDK (one-time vendor), LNRZA (alternative payee), XZEMP (payee in document allowed), CONFS (unconfirmed sensitive change) |
| LFB1 | Vendor company code data | LIFNR, BUKRS, AKONT (reconciliation account), ZTERM, ZWELS, ZAHLS (payment block key), SPERR, LOEVM, REPRF (duplicate invoice check), LNRZB, PERNR, CONFS |
| LFBK | Vendor bank details | LIFNR, BANKS, BANKL, BANKN, KOINH, BVTYP (partner bank type; CVI maps it to BUT0BK-BKVID) |
| BUT000 | Business Partner general data | PARTNER, PARTNER_GUID, NAME_ORG1 |
| BUT0BK | BP bank details | PARTNER, BKVID, BANKS, BANKL, BANKN, IBAN, BK_VALID_FROM, BK_VALID_TO |
| ADRC | Addresses | ADDRNUMBER, NAME1, STREET, CITY1, POST_CODE1, REGION, COUNTRY, PO_BOX |
| DFKKBPTAXNUM | BP tax numbers | PARTNER, TAXTYPE, TAXNUM |
| CVI_VEND_LINK | BP to vendor mapping | PARTNER_GUID, VENDOR |
| ACDOCA | Universal journal | RLDNR, RBUKRS, GJAHR, BELNR, DOCLN, KOART, LIFNR, BLART, BUDAT, HSL, RHCUR, AUGBL, AUGDT, NETDT |
| CDHDR / CDPOS | Change documents | OBJECTCLAS ('KRED'), OBJECTID, CHANGENR, USERNAME, UDATE, UTIME, TABNAME, FNAME, CHNGIND, VALUE_OLD, VALUE_NEW |

Modeling rules:

1. ACDOCA: always filter to the leading ledger (`RLDNR = '0L'`) and vendor lines (`KOART = 'K'`). Otherwise every line is counted once per ledger. Open items are rows where AUGBL is blank. Invoices use BLART 'KR' and payments use 'KZ'. A payment clears invoices by setting their AUGBL and AUGDT.
2. "Blocked" is several fields. Every rule must name which block field it checks.
3. BUT0BK bank rows have validity dates. A future-dated bank row is not a change until it becomes valid. SCD2 logic must respect this.
4. If NETDT is uncertain for a given release, derive the due date from the baseline date plus payment term days and say so in a comment.

## Architecture

1. **Extract replay.** The generator writes N days (default 30) of daily extracts partitioned by `extract_date`. Each day includes full master data snapshots plus that day's journal lines and change documents. Day 1 is the initial load: it carries journal history but no master data changes, and its snapshot is the SCD2 baseline (`initial_load_date` bundle variable, must equal the generator start). The pipeline processes each date idempotently: rerunning a date produces identical output. The generator also plants a few data quality defects (recorded in the manifest as rule `DQ`) so the silver expectations visibly warn and drop.
2. **Bronze.** Raw SAP-shaped tables, loaded as is, with `extract_date` and load metadata columns.
3. **Silver.** Conformed and typed tables: vendor (LFA1 joined to BP through CVI), vendor company code, vendor bank, address, open items, payments. Expectations enforce keys, not-null and accepted values.
4. **Change history.** Build a change feed from CDPOS rows (TABNAME in LFBK, LFA1, LFB1) plus the initial load snapshot, and apply it with AUTO CDC as SCD type 2 into `dim_vendor_bank_scd2` and `dim_vendor_controls_scd2`, sequenced by UDATE plus UTIME and CHANGENR. Bank history comes from vendor-side LFBK changes: CVI only changes LFBK when a BUT0BK row becomes valid, so future-dated BP bank rows are never applied early. BUT0BK change documents (object class BUPA_BUP) stay in bronze for audit.
5. **Gold.** Table names carry a `gold_` prefix so the idempotency check covers them: `gold_exceptions` (one row per rule hit, with rule_id, severity, vendor, company code, evidence columns), `gold_vendor_risk` (ranked), `gold_duplicate_candidates` (matcher output), `gold_run_history` (flag counts per rule per extract_date).

## Control rules

1. **R01 Duplicate vendor.** A matcher pair above threshold where both vendors are active and have open items or recent payments.
2. **R02 Change, pay, revert.** A bank detail change in CDPOS, then a second bank change within N days (default 7), with a clearing payment to that vendor dated between them. A vendor's first-ever bank setup is not a change. Daily snapshots miss this pattern. Change documents catch it.
3. **R03 Shared bank account.** The same BANKS + BANKL + BANKN (or IBAN) on more than one vendor, or on a vendor linked to an employee (LFB1-PERNR populated).
4. **R04 Dormant but not blocked.** No postings for 18 months or more, and none of SPERR, SPERZ or LOEVM set.
5. **R05 Duplicate invoice check disabled.** LFB1-REPRF is blank.
6. **R06 Alternative payee or one-time vendor exposure.** LNRZA or LNRZB populated, or XZEMP set, or a one-time vendor (XCPDK) with repeat payments.
7. **R07 Unconfirmed sensitive change with open items (payment will be held).** CONFS is set and the vendor has open items due within the next payment run window, defined as NETDT on or before extract_date + 7 days (overdue items count).

## Risk score

`exposure = sum of open item amounts (absolute HSL)` for the vendor, and `risk = exposure × max severity tier` across its exceptions, with High = 3, Medium = 2, Low = 1. Document this in the README as a scoring method. Do not invent fractional weights.

## Duplicate matching and evaluation

1. **Normalization:** uppercase, strip punctuation, standardize legal suffixes (INC, LLC, CORP, CO, LTD), collapse whitespace. Normalize addresses the same way.
2. **Blocking:** state plus first three digits of zip, and a second pass on the first token of the normalized name. Report how many candidate pairs blocking produces and what recall it loses.
3. **Baseline:** rapidfuzz token_set_ratio on name, plus address similarity, with a tuned threshold.
4. **Labels:** positive pairs are two vendor records sharing a UEI. Negative pairs are candidate pairs from blocking with different UEIs and different parent UEIs. Same-parent pairs are a separate "related" class. Never sample random pairs for evaluation, because nearly all are non-matches and precision becomes meaningless.
5. **Report:** precision, recall and F1 at the chosen threshold, a precision-recall curve, and a short error analysis with five false positives and five false negatives and why they happened. The evaluation runs on the agency's full vendor population (Interior FY2025-2026, 20,150 rows), with no oversampling.
6. Splink is evaluated on the same folds. Hard cases: 150 pairs in `data/labels/hard_case_candidates.csv`, 100 labeled by Claude following `docs/labeling_guide.md` (disclosed; a person should spot-check).

## Engineering standards

1. Pin all dependency versions.
2. pytest for the generator (seeded determinism, row counts, injected anomaly counts) and for the matching code (normalization, blocking).
3. Expectations in the pipeline for keys and accepted values.
4. Idempotency test: run the pipeline for one extract_date twice and assert identical gold output.
5. Small commits with clear messages.
6. Never commit secrets, tokens or raw downloads.
7. Ask me before deleting files.

## Wording rules for README, comments and dashboard

1. Call it a "vendor master data quality pipeline", with "exception monitoring" and "control testing". Do not use "enterprise product", "fraud detection", "real-time" or "replaces MDG".
2. Name prior art in one line: SAP MDG, Marlin Duplicate Check, Trustpair, Eftsure, apexanalytix.
3. Name the native SAP controls this complements: sensitive fields with dual control (T055F, FK08/FK09 confirmation, the CONFS status), change documents, and the vendor change display.
4. Disclosure line: "Vendor names and addresses are real public data from USAspending.gov. All transactional data is synthetic, shaped to SAP DDIC table and field names, and was not extracted from a live system."
5. Add a Public Cloud note: S/4HANA Public Cloud does not allow table access, so a version for it would read released CDS views such as I_Supplier and I_SupplierCompany.
6. Include a "What I would change with real system access" section: read CDHDR/CDPOS directly, pull F110 payment run data (REGUH/REGUP), and respect bank validity dates from the source.

## README first screen

1. One sentence on the problem: duplicate vendors and last-minute bank changes cost AP teams money.
2. Three numbers: vendors processed, matcher precision and recall at the chosen threshold, open exposure flagged.
3. A screenshot of the Streamlit app, plus a link to the live demo if deployed.
4. A one-line architecture: extract replay → bronze → silver → SCD2 change history → rules and matcher → gold → Streamlit demo.
5. The disclosure line.
6. Setup in as few commands as possible.

Design decisions, limitations and the evaluation details go below the fold.

## Repo layout

```
.
├── CLAUDE.md
├── README.md
├── databricks.yml
├── pyproject.toml
├── extract/            # USAspending fetch and vendor master build (local Python)
├── generator/          # seeded synthetic SAP tables and daily extract replay
├── pipelines/          # Lakeflow pipeline code: bronze, silver, scd2, gold
├── matching/           # normalization, blocking, rapidfuzz matcher, evaluation
├── jobs/               # job definitions referenced by the bundle
├── app/                # Streamlit demo over exported Parquet
├── docs/               # evaluation report
└── tests/
```

## Deferred

1. New rules suggested by review: duplicate or missing tax ID, employee-to-vendor match against HR data (PA0009/PA0006), sensitive-field changes with segregation-of-duties checks, actual duplicate invoices (XBLNR + amount).
2. Real-SAP hardening listed in the README limitations (document types beyond KR/KZ, multiple bank accounts per vendor, clearing resets, currencies).

## How to work with me

1. Work one phase at a time, following the prompt I give you. At the end of each phase, stop, summarize what was built, list anything that failed or was cut, and tell me what I need to do by hand.
2. When a step needs me (creating accounts, logging in, labeling data), give exact numbered steps.
3. If a plan in this file turns out to be wrong or infeasible, tell me directly and propose a fix. Do not silently work around it.
