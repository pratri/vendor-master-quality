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
    "R07": "Unconfirmed change, items due",
}
# Categorical slots 1-6 in fixed order, validated for colour vision deficiency.
RULE_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
TIER_LABEL = {3: "High", 2: "Medium", 1: "Low"}
DISCLOSURE = ("Vendor names and addresses are real public data from USAspending.gov. All "
              "transactional data is synthetic, shaped to SAP DDIC table and field names, and was "
              "not extracted from a live system.")

st.set_page_config(page_title="Vendor master data quality", layout="wide")


@st.cache_resource
def connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()  # in-memory database; views point at the read-only Parquet files
    for f in sorted(DATA.glob("*.parquet")):
        con.execute(f"CREATE VIEW {f.stem} AS SELECT * FROM read_parquet('{f.as_posix()}')")
    return con


@st.cache_data
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return connection().execute(sql, list(params)).df()


def money(x: float) -> str:
    return f"${x:,.0f}"


metrics = json.loads((DATA / "matcher_metrics.json").read_text())
dates = q("SELECT DISTINCT extract_date FROM vendor_risk ORDER BY 1 DESC").extract_date
n_vendors = int(q("SELECT count(*) n FROM vendors WHERE KTOKK = 'KRED'").n[0])

st.title("Vendor master data quality")
st.caption("Exception monitoring and control testing over SAP-shaped vendor master data "
           "(LFA1, LFB1, LFBK, BUT0BK, ACDOCA, CDHDR/CDPOS).")

f1, f2, f3 = st.columns([1, 3, 2])
day = f1.selectbox("Extract date", dates,
                   format_func=lambda d: pd.Timestamp(d).strftime("%Y-%m-%d"))
rules = f2.multiselect("Rules", list(RULES), default=list(RULES),
                       format_func=lambda r: f"{r} {RULES[r]}")
levels = ["High", "Medium", "Low"]
severities = f3.multiselect("Severity", levels, default=levels)

flagged = q(
    """SELECT r.*, list_sort(list(DISTINCT e.rule_id)) AS matched_rules
       FROM vendor_risk r
       JOIN exceptions e ON e.LIFNR = r.LIFNR AND e.extract_date = r.extract_date
       WHERE r.extract_date = ? AND list_contains(?, e.rule_id)
         AND list_contains(?, e.severity)
       GROUP BY ALL ORDER BY r.risk_rank""",
    (day, rules or ["-"], severities or ["-"]))

k1, k2, k3, k4 = st.columns(4)
k1.metric("Vendor records processed", f"{n_vendors:,}")
k2.metric("Vendors with exceptions (filtered)", f"{len(flagged):,}")
k3.metric("Open exposure flagged (filtered)", money(flagged.exposure.sum() if len(flagged) else 0))
t = metrics["test"]
k4.metric(f"Matcher precision / recall (threshold {t['threshold']})",
          f"{t['precision']:.2f} / {t['recall']:.2f}")

ranking, drill, history = st.tabs(["Risk ranking", "Vendor drill-down", "Run history"])

with ranking:
    st.write("Risk = open exposure x highest severity tier (High 3, Medium 2, Low 1). "
             "Pick a row to open it in the drill-down tab.")
    view = flagged.assign(severity=flagged.severity_tier.map(TIER_LABEL),
                          rules=flagged.rules.map(lambda r: ", ".join(r)))
    picked = st.dataframe(
        view[["risk_rank", "LIFNR", "NAME1", "rules", "severity", "exception_count", "exposure",
              "risk"]],
        hide_index=True, width="stretch", height=520, on_select="rerun",
        selection_mode="single-row",
        column_config={
            "risk_rank": st.column_config.NumberColumn("Rank", width="small"),
            "LIFNR": "Vendor", "NAME1": "Name", "rules": "Rules", "severity": "Max severity",
            "exception_count": st.column_config.NumberColumn("Exceptions", width="small"),
            "exposure": st.column_config.NumberColumn("Open exposure", format="dollar"),
            "risk": st.column_config.NumberColumn("Risk", format="dollar"),
        })
    if picked.selection.rows:
        st.session_state["vendor"] = view.iloc[picked.selection.rows[0]].LIFNR

with drill:
    options = list(flagged.LIFNR)
    if not options:
        st.info("No vendors match the current filters.")
    else:
        current = st.session_state.get("vendor")
        vendor = st.selectbox("Vendor", options,
                              index=options.index(current) if current in options else 0,
                              format_func=lambda x: f"{x}  {flagged.set_index('LIFNR').NAME1[x]}")
        v = q("SELECT * FROM vendors WHERE LIFNR = ?", (vendor,)).iloc[0]
        blocks = [f for f in ("SPERR", "SPERZ", "SPERM", "LOEVM") if v[f]] or ["none"]
        st.subheader(f"{v.NAME1}  ({vendor})")
        street = v.STREET or f"PO BOX {v.PO_BOX}"
        st.write(f"{street}, {v.CITY1}, {v.REGION} {v.POST_CODE1}  ·  Business Partner "
                 f"{v.PARTNER or 'missing'}  ·  Blocks: {', '.join(blocks)}  ·  CONFS: "
                 f"{v.CONFS or 'confirmed'}")

        st.markdown("**Exceptions on this date**")
        exc = q("""SELECT rule_id, severity, BUKRS, detail FROM exceptions
                   WHERE LIFNR = ? AND extract_date = ? ORDER BY rule_id""", (vendor, day))
        st.dataframe(exc, hide_index=True, width="stretch")

        left, right = st.columns(2)
        with left:
            st.markdown("**Bank account history** (SCD type 2 from change documents)")
            st.dataframe(q("""SELECT strftime(valid_from, '%Y-%m-%d %H:%M') AS valid_from,
                                     strftime(valid_to, '%Y-%m-%d %H:%M') AS valid_to,
                                     BANKL, BANKN, source, changed_by
                              FROM bank_history WHERE LIFNR = ? ORDER BY 1""",
                           (vendor,)), hide_index=True, width="stretch")
            st.markdown("**Control field changes** (LFA1 / LFB1)")
            st.dataframe(q("""SELECT strftime(valid_from, '%Y-%m-%d %H:%M') AS valid_from, BUKRS,
                                     LFA1_SPERZ, LFA1_CONFS, LFB1_REPRF, LFB1_ZAHLS, source,
                                     changed_by
                              FROM controls_history WHERE LIFNR = ? ORDER BY 1, BUKRS""",
                           (vendor,)), hide_index=True, width="stretch")
        with right:
            items = q("""SELECT BUKRS, BELNR, strftime(BUDAT, '%Y-%m-%d') AS BUDAT,
                                strftime(NETDT, '%Y-%m-%d') AS NETDT, amount FROM open_items
                         WHERE LIFNR = ? AND extract_date = ? ORDER BY NETDT""", (vendor, day))
            st.markdown(f"**Open items on this date** ({money(items.amount.sum())})")
            st.dataframe(items, hide_index=True, width="stretch",
                         column_config={"amount": st.column_config.NumberColumn(format="dollar")})
            st.markdown("**Duplicate candidates** (matcher)")
            st.dataframe(q("""SELECT CASE WHEN LIFNR_A = ? THEN LIFNR_B ELSE LIFNR_A END AS other,
                                     CASE WHEN LIFNR_A = ? THEN NAME1_B ELSE NAME1_A END AS name,
                                     round(score, 1) AS score, above_threshold
                              FROM duplicate_candidates WHERE ? IN (LIFNR_A, LIFNR_B)
                              ORDER BY score DESC""", (vendor, vendor, vendor)),
                           hide_index=True, width="stretch")

with history:
    # Fill days with no flags as zeros so every line spans the whole window.
    rh = q("""SELECT d.extract_date, r.rule_id, coalesce(h.exceptions, 0) AS exceptions,
                     coalesce(h.vendors_flagged, 0) AS vendors_flagged,
                     coalesce(h.new_vendors, 0) AS new_vendors,
                     coalesce(h.exposure_flagged, 0) AS exposure_flagged
              FROM (SELECT DISTINCT extract_date FROM vendor_risk) d
              CROSS JOIN (SELECT DISTINCT rule_id FROM exceptions) r
              LEFT JOIN run_history h USING (extract_date, rule_id)
              ORDER BY extract_date, rule_id""")
    rh = rh[rh.rule_id.isin(rules)] if rules else rh.iloc[0:0]
    rh["rule"] = rh.rule_id + " " + rh.rule_id.map(RULES)
    st.write("Vendors flagged per rule per extract date. Each rule has its own scale, because "
             "R01 flags thousands while the others flag tens.")
    order = [f"{r} {RULES[r]}" for r in RULES]
    base = alt.Chart(rh).encode(
        x=alt.X("extract_date:T", title=None, axis=alt.Axis(format="%b %d", labelColor="#898781",
                                                            gridColor="#e1e0d9")),
        y=alt.Y("vendors_flagged:Q", title=None, scale=alt.Scale(zero=True),
                axis=alt.Axis(labelColor="#898781", gridColor="#e1e0d9")),
        color=alt.Color("rule:N", scale=alt.Scale(domain=order, range=RULE_COLORS), legend=None),
        tooltip=[alt.Tooltip("extract_date:T", title="Date", format="%Y-%m-%d"),
                 alt.Tooltip("rule:N", title="Rule"),
                 alt.Tooltip("vendors_flagged:Q", title="Vendors flagged", format=","),
                 alt.Tooltip("new_vendors:Q", title="New that day", format=","),
                 alt.Tooltip("exposure_flagged:Q", title="Exposure flagged", format="$,.0f")])
    chart = (alt.layer(base.mark_line(strokeWidth=2),
                       base.mark_point(size=64, filled=True, stroke="#fcfcfb", strokeWidth=2))
             .properties(width=300, height=150)
             .facet(facet=alt.Facet("rule:N", sort=order, title=None,
                                    header=alt.Header(labelColor="#0b0b0b", labelFontSize=12,
                                                      labelAnchor="start")), columns=3)
             .resolve_scale(y="independent")
             .configure_view(stroke=None))
    st.altair_chart(chart)
    st.dataframe(rh[["extract_date", "rule", "exceptions", "vendors_flagged", "new_vendors",
                     "exposure_flagged"]], hide_index=True, width="stretch",
                 column_config={
                     "extract_date": st.column_config.DateColumn("Extract date",
                                                                 format="YYYY-MM-DD"),
                     "exposure_flagged": st.column_config.NumberColumn("Exposure flagged",
                                                                       format="dollar")})

st.divider()
st.caption(DISCLOSURE)
st.caption("Duplicate matching is evaluated on the real names (see docs/evaluation.md). Control "
           "exceptions are injected by a seeded generator at known rates and reconciled exactly.")
