"""Vendor master data quality pipeline: exception monitoring demo.

Reads the gold tables exported from Databricks (app/data/*.parquet) with DuckDB, in memory.
The Parquet files are only ever read. Run: streamlit run app/streamlit_app.py
"""

import json
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DATA = Path(__file__).parent / "data"
RULES = {
    "R01": "Duplicate vendor",
    "R02": "Change, pay, revert",
    "R03": "Shared bank account",
    "R04": "Dormant but not blocked",
    "R05": "Duplicate invoice check off",
    "R06": "Alternative payee / one-time vendor",
    "R07": "Payment will be held (unconfirmed change)",
}
# Categorical slots in fixed order, validated for colour vision deficiency.
RULE_COLORS = dict(zip(RULES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#4a3aa7",
                               "#008300"], strict=True))
TIER = {"High": 3, "Medium": 2, "Low": 1}
QUEUES = ["1 Act before next payment run", "2 Investigate", "3 Master data cleanup"]
DISCLOSURE = ("Vendor names and addresses are real public data from USAspending.gov. All "
              "transactional data is synthetic, shaped to SAP DDIC table and field names, and was "
              "not extracted from a live system.")
GLOSSARY = {
    "Vendor (LIFNR)": "SAP vendor number. S/4HANA calls vendors suppliers.",
    "Company code (BUKRS)": "Legal entity that books the invoice (1000, 2000, 3000 here).",
    "Routing / account (BANKL / BANKN)": "Bank key and account number in LFBK. Obviously fake.",
    "CONFS": "Status of a sensitive-field change (e.g. bank). 1 = not yet confirmed (FK08).",
    "SPERR / SPERZ / SPERM / LOEVM": "Posting block / payment block / purchasing block / "
                                     "deletion flag on the vendor.",
    "REPRF": "Duplicate invoice check flag in LFB1. Blank = the check is off.",
    "LNRZA / LNRZB / XZEMP": "Alternative payee (vendor / company code) and 'payee in document "
                             "allowed'.",
    "XCPDK": "One-time vendor account: name and bank are typed on each document.",
    "Open exposure": "Sum of unpaid vendor invoice amounts (ACDOCA, leading ledger, AUGBL blank).",
    "Priority score": "Open exposure x highest severity tier (High 3, Medium 2, Low 1). Unitless.",
}

st.set_page_config(page_title="Vendor master data quality", layout="wide")


# Cache keys include the data files' sizes and times, so new exported data is never served
# stale results (Streamlit Cloud hot-reloads code on push without restarting the process).
DATA_VERSION = str(sorted((f.name, f.stat().st_size, f.stat().st_mtime_ns)
                          for f in DATA.glob("*")))


@st.cache_resource
def connection(version: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()  # in-memory database; views point at the read-only Parquet files
    for f in sorted(DATA.glob("*.parquet")):
        con.execute(f"CREATE VIEW {f.stem} AS SELECT * FROM read_parquet('{f.as_posix()}')")
    return con


@st.cache_data
def cached_query(sql: str, params: tuple, version: str) -> pd.DataFrame:
    return connection(version).execute(sql, list(params)).df()


def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return cached_query(sql, params, DATA_VERSION)


def money(x: float) -> str:
    return f"${x:,.0f}"


def compact_money(x: float) -> str:
    """Short form for headline tiles, so values are not clipped on narrow screens."""
    return f"${x / 1e6:,.1f}M" if x >= 1e6 else f"${x / 1e3:,.0f}K" if x >= 1e3 else money(x)


def show_value(v) -> str:
    s = str(v)
    if len(s) >= 19 and s[10:11] == "T":  # ISO timestamp from the evidence JSON
        return s[:16].replace("T", " ")
    return s


def queue_of(rule: str, severity: str, score: float | None) -> str:
    if rule == "R02" or (rule == "R03" and severity == "High"):
        return QUEUES[0]
    if rule in ("R03", "R06", "R07") or (rule == "R01" and (score or 0) >= 95):
        return QUEUES[1]
    return QUEUES[2]


@st.cache_data
def exceptions_on(day, version: str) -> pd.DataFrame:
    """Exceptions on a date with first-flagged date, open exposure and a work queue."""
    df = q("""WITH first AS (
                SELECT rule_id, LIFNR, BUKRS, min(extract_date) AS first_flagged
                FROM exceptions WHERE extract_date <= ? GROUP BY ALL)
              SELECT e.rule_id, e.severity, e.LIFNR, e.BUKRS, e.related_lifnr, e.detail,
                     e.evidence, f.first_flagged, coalesce(r.exposure, 0) AS exposure,
                     r.NAME1, try_cast(json_extract_string(e.evidence, '$.score') AS DOUBLE)
                       AS match_score
              FROM exceptions e
              JOIN first f USING (rule_id, LIFNR, BUKRS)
              LEFT JOIN vendor_risk r ON r.LIFNR = e.LIFNR AND r.extract_date = e.extract_date
              WHERE e.extract_date = ?""", (day, day))
    df["queue"] = [queue_of(r, s, m) for r, s, m in zip(df.rule_id, df.severity, df.match_score,
                                                        strict=True)]
    df["days_open"] = (pd.Timestamp(day) - pd.to_datetime(df.first_flagged)).dt.days
    df["tier"] = df.severity.map(TIER)
    return df.sort_values(["queue", "tier", "first_flagged", "exposure"],
                          ascending=[True, False, False, False]).reset_index(drop=True)


def vendor_names(ids) -> dict:
    ids = [i for i in ids if i]
    if not ids:
        return {}
    d = q("SELECT LIFNR, NAME1 FROM vendors WHERE list_contains(?, LIFNR)", (ids,))
    return dict(zip(d.LIFNR, d.NAME1, strict=True))


def render_vendor(vendor: str, day) -> None:
    """Everything needed to close or escalate the vendor's exceptions."""
    v = q("SELECT * FROM vendors WHERE LIFNR = ?", (vendor,)).iloc[0]
    blocks = [f for f in ("SPERR", "SPERZ", "SPERM", "LOEVM") if v[f]] or ["none"]
    street = v.STREET or (f"PO BOX {v.PO_BOX}" if v.PO_BOX else "address on each document")
    st.subheader(f"{v.NAME1}  ·  vendor {vendor}")
    st.caption(f"{street}, {v.CITY1} {v.REGION} {v.POST_CODE1}  ·  account group {v.KTOKK}  ·  "
               f"Business Partner {v.PARTNER or 'missing'}  ·  blocks: {', '.join(blocks)}  ·  "
               f"sensitive change (CONFS): {'unconfirmed' if v.CONFS else 'confirmed'}")

    exc = q("""SELECT rule_id, severity, BUKRS, related_lifnr, detail, evidence FROM exceptions
               WHERE LIFNR = ? AND extract_date = ? ORDER BY rule_id""", (vendor, day))
    names = vendor_names([x for r in exc.related_lifnr for x in r.split(",")])
    for r in exc.itertuples():
        cc = f" · company code {r.BUKRS}" if r.BUKRS else ""
        st.markdown(f"**{r.rule_id} {RULES[r.rule_id]}** · {r.severity}{cc}  \n{r.detail}")
        related = [f"{x} {names.get(x, '')}".strip() for x in r.related_lifnr.split(",") if x]
        evidence = {k: v for k, v in json.loads(r.evidence).items() if v not in (None, "")}
        if related:
            evidence["related vendors"] = "; ".join(related)
        with st.expander("Evidence"):
            st.table(pd.DataFrame({"Field": [k.replace("_", " ") for k in evidence],
                                   "Value": [show_value(x) for x in evidence.values()]}))

    left, right = st.columns(2)
    with left:
        st.markdown("**Bank account history** (SCD type 2 built from change documents)")
        st.dataframe(q("""SELECT strftime(valid_from, '%Y-%m-%d %H:%M') AS "Valid from",
                                 coalesce(strftime(valid_to, '%Y-%m-%d %H:%M'), 'current')
                                   AS "Valid to",
                                 BANKL AS "Routing", BANKN AS "Account", source AS "Source",
                                 changed_by AS "Changed by"
                          FROM bank_history WHERE LIFNR = ? ORDER BY valid_from""", (vendor,)),
                       hide_index=True, width="stretch")
        st.markdown("**Control field changes** (LFA1 / LFB1)")
        st.dataframe(q("""SELECT strftime(valid_from, '%Y-%m-%d %H:%M') AS "From",
                                 BUKRS AS "Company code", LFA1_SPERZ AS "Payment block",
                                 LFA1_CONFS AS "CONFS", LFB1_REPRF AS "Dup. invoice check",
                                 source AS "Source", changed_by AS "Changed by"
                          FROM controls_history WHERE LIFNR = ? ORDER BY 1, 2""", (vendor,)),
                       hide_index=True, width="stretch")
    with right:
        items = q("""SELECT BUKRS AS "Company code", BELNR AS "Document",
                            strftime(BUDAT, '%Y-%m-%d') AS "Posted",
                            strftime(NETDT, '%Y-%m-%d') AS "Due", amount AS "Amount"
                     FROM open_items WHERE LIFNR = ? AND extract_date = ? ORDER BY NETDT""",
                  (vendor, day))
        st.markdown(f"**Open items on this date** ({money(items.Amount.sum())})")
        st.dataframe(items, hide_index=True, width="stretch",
                     column_config={"Amount": st.column_config.NumberColumn(format="dollar")})
        st.markdown("**Possible duplicates** (matcher score 0-100; 91+ flags R01)")
        st.dataframe(q("""SELECT CASE WHEN LIFNR_A = ? THEN LIFNR_B ELSE LIFNR_A END AS "Vendor",
                                 CASE WHEN LIFNR_A = ? THEN NAME1_B ELSE NAME1_A END AS "Name",
                                 round(score, 1) AS "Score", above_threshold AS "Flagged"
                          FROM duplicate_candidates WHERE ? IN (LIFNR_A, LIFNR_B)
                          ORDER BY score DESC""", (vendor, vendor, vendor)),
                       hide_index=True, width="stretch")


metrics = json.loads((DATA / "matcher_metrics.json").read_text())
dates = q("SELECT DISTINCT extract_date FROM vendor_risk ORDER BY 1 DESC").extract_date
n_vendors = int(q("SELECT count(*) n FROM vendors WHERE KTOKK = 'KRED'").n[0])

st.title("Vendor master data quality")
st.caption("Exception monitoring over SAP-shaped vendor master data: change documents, bank "
           "history, control fields and duplicate vendors. Built on Databricks (Lakeflow, "
           "AUTO CDC SCD2, Unity Catalog).")

f1, f2, f3 = st.columns([1, 4, 2])
day = f1.selectbox("Extract date", dates,
                   format_func=lambda d: pd.Timestamp(d).strftime("%Y-%m-%d"))
rules = f2.pills("Rules", list(RULES), default=list(RULES), selection_mode="multi",
                 format_func=lambda r: r, help="\n".join(f"{k}: {v}" for k, v in RULES.items()))
severities = f3.pills("Severity", list(TIER), default=list(TIER), selection_mode="multi")

exc = exceptions_on(day, DATA_VERSION)
shown = exc[exc.rule_id.isin(rules or []) & exc.severity.isin(severities or [])]
t = metrics["test"]
k1, k2, k3, k4 = st.columns(4)
k1.metric("Vendor records (real names)", f"{n_vendors:,}")
k2.metric("Vendors with exceptions", f"{shown.LIFNR.nunique():,}")
k3.metric("Open exposure flagged (synthetic)",
          compact_money(shown.drop_duplicates("LIFNR").exposure.sum()))
k4.metric("Matcher precision / recall", f"{t['precision']:.2f} / {t['recall']:.2f}",
          help=f"Held-out pairs, score threshold {t['threshold']} of 100")

tiles = st.columns(len(RULES))
prev_day = pd.Timestamp(day) - pd.Timedelta(days=1)
for col, (rule, label) in zip(tiles, RULES.items(), strict=True):
    today = exc[exc.rule_id == rule].LIFNR.nunique()
    new = int((exc[(exc.rule_id == rule)].drop_duplicates("LIFNR").first_flagged
               .pipe(pd.to_datetime) > prev_day).sum())
    col.metric(f"{rule} {label}", f"{today:,}", f"+{new} new" if new else None,
               delta_color="inverse")

queue_tab, ranking_tab, drill_tab, history_tab, about_tab = st.tabs(
    ["Work queue", "Risk ranking", "Vendor drill-down", "Run history", "About"])

with queue_tab:
    # Prefer a same-day change, pay, revert: the case an end-of-day snapshot cannot see.
    r02 = exc[exc.rule_id == "R02"].assign(same_day=lambda d: d.detail.str.contains("08:15"))
    r02 = r02.sort_values(["same_day", "LIFNR"], ascending=[False, True])
    if len(r02):
        st.info(f"Start here: vendor **{r02.LIFNR.iloc[0]}** ({r02.NAME1.iloc[0]}): "
                f"{r02.detail.iloc[0]}. Pick it below, or any row, to see the evidence.")
    counts = shown.groupby("queue").size()
    st.write("  ·  ".join(f"**{qn[2:]}**: {counts.get(qn, 0):,}" for qn in QUEUES))
    view = shown.assign(rule=shown.rule_id + " " + shown.rule_id.map(RULES),
                        queue_name=shown.queue.str[2:])
    picked = st.dataframe(
        view[["queue_name", "rule", "severity", "LIFNR", "NAME1", "detail", "first_flagged",
              "days_open", "exposure"]],
        hide_index=True, width="stretch", height=380, on_select="rerun",
        selection_mode="single-row", key="queue_table",
        column_config={
            "queue_name": "Queue", "rule": "Rule", "severity": "Severity", "LIFNR": "Vendor",
            "NAME1": "Name", "detail": st.column_config.TextColumn("Detail", width="large"),
            "first_flagged": st.column_config.DateColumn("First flagged", format="YYYY-MM-DD"),
            "days_open": st.column_config.NumberColumn("Days open", width="small"),
            "exposure": st.column_config.NumberColumn("Open exposure", format="dollar"),
        })
    if picked.selection.rows:
        vendor = view.iloc[picked.selection.rows[0]].LIFNR
        st.session_state["vendor"] = vendor
        st.divider()
        render_vendor(vendor, day)

with ranking_tab:
    st.write("Vendors ranked by priority score = open exposure x highest severity tier. "
             "Duplicates (R01) dominate by volume; use the filters above to focus.")
    risk = q("SELECT * FROM vendor_risk WHERE extract_date = ? ORDER BY risk_rank", (day,))
    risk = risk[risk.LIFNR.isin(shown.LIFNR)].reset_index(drop=True)
    risk.insert(0, "rank", range(1, len(risk) + 1))
    risk["rules"] = risk.rules.map(", ".join)
    risk["severity"] = risk.severity_tier.map({v: k for k, v in TIER.items()})
    picked = st.dataframe(
        risk[["rank", "risk_rank", "LIFNR", "NAME1", "rules", "severity", "exception_count",
              "exposure", "risk"]],
        hide_index=True, width="stretch", height=420, on_select="rerun",
        selection_mode="single-row", key="risk_table",
        column_config={
            "rank": st.column_config.NumberColumn("Rank", width="small"),
            "risk_rank": st.column_config.NumberColumn("Overall rank", width="small"),
            "LIFNR": "Vendor", "NAME1": "Name", "rules": "Rules", "severity": "Max severity",
            "exception_count": st.column_config.NumberColumn("Exceptions", width="small"),
            "exposure": st.column_config.NumberColumn("Open exposure", format="dollar"),
            "risk": st.column_config.NumberColumn("Priority score", format="%.0f"),
        })
    if picked.selection.rows:
        vendor = risk.iloc[picked.selection.rows[0]].LIFNR
        st.session_state["vendor"] = vendor
        st.divider()
        render_vendor(vendor, day)

with drill_tab:
    options = sorted(shown.LIFNR.unique())
    if not options:
        st.info("No vendors match the current filters.")
    else:
        current = st.session_state.get("vendor")
        label = dict(zip(shown.LIFNR, shown.NAME1, strict=False))
        vendor = st.selectbox("Vendor", options,
                              index=options.index(current) if current in options else 0,
                              format_func=lambda x: f"{x}  {label.get(x, '')}")
        render_vendor(vendor, day)

with history_tab:
    st.write("Vendors flagged per rule per extract date, each rule on its own scale. "
             "R02 keeps a completed pattern flagged for 30 days, so its line accumulates. "
             "Weekends carry Friday's flags (no postings).")
    rh = q("""SELECT d.extract_date, r.rule_id, coalesce(h.vendors_flagged, 0) AS vendors_flagged,
                     coalesce(h.new_vendors, 0) AS new_vendors,
                     coalesce(h.exposure_flagged, 0) AS exposure_flagged
              FROM (SELECT DISTINCT extract_date FROM vendor_risk) d
              CROSS JOIN (SELECT DISTINCT rule_id FROM exceptions) r
              LEFT JOIN run_history h USING (extract_date, rule_id)
              ORDER BY extract_date, rule_id""")
    grid = st.columns(2)
    for i, rule in enumerate([r for r in RULES if r in (rules or [])]):
        data = rh[rh.rule_id == rule]
        base = alt.Chart(data).encode(
            x=alt.X("extract_date:T", title=None,
                    axis=alt.Axis(format="%b %d", labelColor="#898781", gridColor="#e1e0d9")),
            y=alt.Y("vendors_flagged:Q", title=None, scale=alt.Scale(zero=True),
                    axis=alt.Axis(labelColor="#898781", gridColor="#e1e0d9")),
            tooltip=[alt.Tooltip("extract_date:T", title="Date", format="%Y-%m-%d"),
                     alt.Tooltip("vendors_flagged:Q", title="Vendors flagged", format=","),
                     alt.Tooltip("new_vendors:Q", title="New that day", format=","),
                     alt.Tooltip("exposure_flagged:Q", title="Exposure", format="$,.0f")])
        chart = (alt.layer(base.mark_line(strokeWidth=2, color=RULE_COLORS[rule]),
                           base.mark_point(size=48, filled=True, color=RULE_COLORS[rule],
                                           stroke="#fcfcfb", strokeWidth=2))
                 .properties(title=f"{rule} {RULES[rule]}", height=170)
                 .configure_view(stroke=None).configure_title(anchor="start", fontSize=13))
        grid[i % 2].altair_chart(chart, use_container_width=True)
    with st.expander("Table view"):
        st.dataframe(rh.assign(rule=rh.rule_id + " " + rh.rule_id.map(RULES)),
                     hide_index=True, width="stretch",
                     column_config={
                         "extract_date": st.column_config.DateColumn("Extract date",
                                                                     format="YYYY-MM-DD"),
                         "exposure_flagged": st.column_config.NumberColumn(
                             "Exposure flagged", format="dollar")})

with about_tab:
    st.markdown(
        "**What this is.** A data engineering prototype. Seeded SAP-shaped extracts are replayed "
        "daily through a Lakeflow pipeline on Databricks: bronze (Auto Loader), silver "
        "(expectations), SCD type 2 change history (AUTO CDC from CDHDR/CDPOS), then rules and a "
        "duplicate matcher into gold. This app reads the exported gold tables.\n\n"
        "**How it is checked.** Every control exception is injected by the generator at a known "
        "rate, with look-alike decoys. The pipeline's hits match the generator's injected cases "
        "exactly (this proves the pipeline logic, not detection on real data). Rerunning a date or "
        "rebuilding from raw files leaves every output table identical. The duplicate matcher is "
        "evaluated on the real vendor names against a UEI answer key.\n\n"
        "**Work queues.** 1: R02 change-pay-revert and employee-linked shared bank accounts. "
        "2: unconfirmed sensitive changes (payment is held until confirmed), other shared "
        "accounts, alternative payees and one-time vendors, strong duplicate pairs (score 95+). "
        "3: other duplicates, dormant vendors, duplicate invoice check off.")
    st.markdown("**Glossary**")
    st.table(pd.DataFrame({"Term": list(GLOSSARY), "Meaning": list(GLOSSARY.values())}))

st.divider()
st.caption(DISCLOSURE)
