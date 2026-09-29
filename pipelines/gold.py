"""Gold: control rules, exceptions, vendor risk and run history, evaluated per extract_date.

Each rule is its own view, named after the rule, and states which SAP fields it reads.
"Blocked" is several fields in SAP, so every rule names the block fields it checks.
"""

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MATCHER_OUTPUT = spark.conf.get("matcher_output")  # noqa: F821
SEVERITY_TIER = {"High": 3, "Medium": 2, "Low": 1}
R01_RECENT_PAYMENT_DAYS = 90
R02_CHANGE_WINDOW_DAYS = 7  # second bank change within this many days of the first
R02_LOOKBACK_DAYS = 30  # a completed pattern stays flagged this long
R04_DORMANT_MONTHS = 18
R07_PAYMENT_WINDOW_DAYS = 7  # NETDT on or before extract_date + 7 (overdue counts)
R08_PAYMENT_WINDOW_DAYS = 14  # payment within this many days after a bank change
R08_LOOKBACK_DAYS = 30  # flagged for this many days after the change


def read(name: str) -> DataFrame:
    return spark.read.table(name)  # noqa: F821


def extract_dates() -> DataFrame:
    return read("silver_vendor").select("extract_date").distinct()


def exceptions(df: DataFrame, rule_id: str, severity: str | Column, detail: Column,
               evidence: list[Column], bukrs: Column | None = None,
               related: Column | None = None) -> DataFrame:
    """Shape a rule's hits into the common exceptions layout."""
    sev = severity if isinstance(severity, Column) else F.lit(severity)
    return df.select(
        "extract_date", F.lit(rule_id).alias("rule_id"), sev.alias("severity"), "LIFNR",
        (bukrs if bukrs is not None else F.lit("")).alias("BUKRS"),
        (related if related is not None else F.lit("")).alias("related_lifnr"),
        detail.alias("detail"), F.to_json(F.struct(*evidence)).alias("evidence"))


@dp.view(comment="R02 change, pay, revert. Reads LFBK change documents (dim_vendor_bank_scd2) "
                 "and vendor payments (ACDOCA BLART KZ). No block fields.")
def rule_r02_change_pay_revert():
    w = Window.partitionBy("LIFNR").orderBy("start_ts", "CHANGENR")
    versions = (read("dim_vendor_bank_scd2")
                .select("LIFNR", "BANKN", "CHANGENR", "changed_by", "source",
                        F.col("__START_AT.change_ts").alias("start_ts"))
                .withColumn("prev_bankn", F.lag("BANKN").over(w))
                .withColumn("next_ts", F.lead("start_ts").over(w))
                .withColumn("next_bankn", F.lead("BANKN").over(w))
                .withColumn("next_by", F.lead("changed_by").over(w))
                .withColumn("next_changenr", F.lead("CHANGENR").over(w))
                # prev_bankn: a first-ever bank setup is not a change of bank
                .where((F.col("source") == "CDPOS") & F.col("prev_bankn").isNotNull()
                       & F.col("next_ts").isNotNull()
                       & (F.col("next_ts") <= F.col("start_ts")
                          + F.expr(f"INTERVAL {R02_CHANGE_WINDOW_DAYS} DAYS"))))
    pays = read("silver_payments").select("LIFNR", "BUKRS", "BELNR", "BUDAT", "HSL")
    between = (F.col("BUDAT") >= F.to_date("start_ts")) & (F.col("BUDAT") <= F.to_date("next_ts"))
    hits = (versions.join(pays, "LIFNR").where(between)
            .groupBy("LIFNR", "start_ts", "changed_by", "CHANGENR", "BANKN", "prev_bankn",
                     "next_ts", "next_by", "next_changenr", "next_bankn")
            .agg(F.min("BUKRS").alias("BUKRS"), F.min("BELNR").alias("payment_belnr"),
                 F.min("BUDAT").alias("payment_date"), F.sum("HSL").alias("payment_amount")))
    completed = F.to_date("next_ts")
    hits = extract_dates().join(hits, (completed <= F.col("extract_date")) & (
        completed >= F.date_sub("extract_date", R02_LOOKBACK_DAYS)))
    detail = F.format_string("Bank changed %s by %s, paid %s on %s, changed again %s by %s",
                             F.date_format("start_ts", "yyyy-MM-dd HH:mm"), "changed_by",
                             F.format_number("payment_amount", 2),
                             F.date_format("payment_date", "yyyy-MM-dd"),
                             F.date_format("next_ts", "yyyy-MM-dd HH:mm"), "next_by")
    evidence = [F.col("start_ts").alias("first_change_ts"), F.col("changed_by").alias("first_by"),
                F.col("CHANGENR").alias("first_changenr"),
                F.col("prev_bankn").alias("original_bankn"),
                F.col("BANKN").alias("temporary_bankn"), "payment_belnr", "payment_date",
                "payment_amount", F.col("next_ts").alias("second_change_ts"),
                F.col("next_by").alias("second_by"),
                F.col("next_changenr").alias("second_changenr"),
                (F.col("next_bankn") == F.col("prev_bankn")).alias("reverted_to_original")]
    return exceptions(hits, "R02", "High", detail, evidence, bukrs=F.col("BUKRS"))


@dp.view(comment="R08 payment soon after a bank change. Reads LFBK change documents "
                 "(dim_vendor_bank_scd2) and vendor payments (ACDOCA BLART KZ). No block fields.")
def rule_r08_payment_after_bank_change():
    w = Window.partitionBy("LIFNR").orderBy("start_ts", "CHANGENR")
    changes = (read("dim_vendor_bank_scd2")
               .select("LIFNR", "BANKN", "CHANGENR", "changed_by", "source",
                       F.col("__START_AT.change_ts").alias("start_ts"))
               .withColumn("prev_bankn", F.lag("BANKN").over(w))
               .where((F.col("source") == "CDPOS") & F.col("prev_bankn").isNotNull())
               .withColumn("change_date", F.to_date("start_ts")))
    # BUDAT has no time, so a payment on the day of the change counts.
    pays = read("silver_payments").select("LIFNR", "BUKRS", "BELNR", "BUDAT", "HSL")
    hits = changes.join(pays, "LIFNR").where(F.col("BUDAT").between(
        F.col("change_date"), F.date_add("change_date", R08_PAYMENT_WINDOW_DAYS)))
    hits = (extract_dates().join(hits, (F.col("BUDAT") <= F.col("extract_date")) & (
                F.col("extract_date") <= F.date_add("change_date", R08_LOOKBACK_DAYS)))
            .groupBy("extract_date", "LIFNR", "start_ts", "changed_by", "CHANGENR", "BANKN",
                     "prev_bankn")
            .agg(F.min(F.struct("BUDAT", "BELNR", "BUKRS")).alias("first"),
                 F.count("*").alias("payments"), F.sum("HSL").alias("paid_amount"))
            .select("*", F.col("first.BUKRS").alias("BUKRS"),
                    F.col("first.BELNR").alias("first_payment_belnr"),
                    F.col("first.BUDAT").alias("first_payment_date")))
    detail = F.format_string("Bank changed %s by %s, then %d payment(s) totalling %s from %s",
                             F.date_format("start_ts", "yyyy-MM-dd HH:mm"), "changed_by",
                             "payments", F.format_number("paid_amount", 2),
                             F.date_format("first_payment_date", "yyyy-MM-dd"))
    evidence = [F.col("start_ts").alias("change_ts"), "changed_by", "CHANGENR",
                F.col("prev_bankn").alias("old_bankn"), F.col("BANKN").alias("new_bankn"),
                "first_payment_belnr", "first_payment_date", "payments", "paid_amount"]
    return exceptions(hits, "R08", "Medium", detail, evidence, bukrs=F.col("BUKRS"))


@dp.view(comment="R03 shared bank account. Reads LFBK (BANKS, BANKL, BANKN) and LFB1-PERNR "
                 "(employee link). US banks carry no IBAN, so the BANKS/BANKL/BANKN key is used.")
def rule_r03_shared_bank():
    bank = read("silver_vendor_bank").select("extract_date", "LIFNR", "BANKS", "BANKL", "BANKN")
    employee = (read("silver_vendor_company_code").where(F.col("PERNR") != "")
                .select("extract_date", "LIFNR").distinct()
                .withColumn("employee_linked", F.lit(True)))
    g = Window.partitionBy("extract_date", "BANKS", "BANKL", "BANKN")
    hits = (bank.join(employee, ["extract_date", "LIFNR"], "left")
            .withColumn("holders", F.array_sort(F.collect_set("LIFNR").over(g)))
            .withColumn("group_has_employee", F.max(F.coalesce("employee_linked",
                                                                F.lit(False))).over(g))
            .where(F.size("holders") > 1))
    others = F.array_join(F.array_remove("holders", F.col("LIFNR")), ",")
    detail = F.format_string("Bank %s/%s also used by %s%s", "BANKL", "BANKN", others,
                             F.when(F.col("group_has_employee"), F.lit(" (employee-linked)"))
                             .otherwise(F.lit("")))
    evidence = ["BANKS", "BANKL", "BANKN", "holders", "group_has_employee"]
    severity = F.when(F.col("group_has_employee"), "High").otherwise("Medium")
    return exceptions(hits, "R03", severity, detail, evidence, related=others)


@dp.view(comment="R04 dormant but not blocked. Reads ACDOCA postings and LFA1 SPERR (posting "
                 "block), SPERZ (payment block), LOEVM (deletion flag). SPERM alone does not "
                 "count as blocked.")
def rule_r04_dormant_not_blocked():
    v = read("silver_vendor").select("extract_date", "LIFNR", "SPERR", "SPERZ", "SPERM",
                                     "LOEVM", "created_on")
    lines = read("silver_journal_lines").select("LIFNR", "BUDAT", "first_extract_date")
    last = (v.select("extract_date", "LIFNR").join(lines, "LIFNR")
            .where(F.col("first_extract_date") <= F.col("extract_date"))
            .groupBy("extract_date", "LIFNR").agg(F.max("BUDAT").alias("last_posting")))
    hits = (v.join(last, ["extract_date", "LIFNR"], "left")
            .withColumn("last_activity", F.coalesce("last_posting", "created_on"))
            # "18 months or more", so the boundary day counts
            .where((F.col("last_activity") <= F.add_months("extract_date", -R04_DORMANT_MONTHS))
                   & (F.col("SPERR") == "") & (F.col("SPERZ") == "") & (F.col("LOEVM") == "")))
    months = F.floor(F.months_between("extract_date", "last_activity")).cast("int")
    detail = F.format_string("No postings since %s (%d months); SPERR, SPERZ, LOEVM not set%s",
                             F.date_format("last_activity", "yyyy-MM-dd"), months,
                             F.when(F.col("SPERM") != "", F.lit(", only SPERM set"))
                             .otherwise(F.lit("")))
    evidence = ["last_posting", "last_activity", months.alias("months_dormant"), "SPERM"]
    return exceptions(hits, "R04", "Low", detail, evidence)


@dp.view(comment="R05 duplicate invoice check disabled. Reads LFB1-REPRF. No block fields.")
def rule_r05_duplicate_invoice_check_off():
    hits = read("silver_vendor_company_code").where(F.col("REPRF") == "")
    detail = F.format_string("LFB1-REPRF blank in company code %s", "BUKRS")
    return exceptions(hits, "R05", "Medium", detail, ["REPRF", "BUKRS", "ZWELS"],
                      bukrs=F.col("BUKRS"))


@dp.view(comment="R07 payment will be held: unconfirmed sensitive change with items due. "
                 "Reads LFA1-CONFS, LFB1-CONFS and open items (ACDOCA AUGBL blank, NETDT). "
                 "F110 already holds these payments, so the job is to confirm or reject the "
                 "change before the due date; Medium, not High.")
def rule_r07_unconfirmed_change_items_due():
    v = read("silver_vendor").select("extract_date", "LIFNR", F.col("CONFS").alias("lfa1_confs"))
    cc = read("silver_vendor_company_code").select("extract_date", "LIFNR", "BUKRS",
                                                   F.col("CONFS").alias("lfb1_confs"))
    due = (read("silver_open_items")
           .where(F.col("NETDT") <= F.date_add("extract_date", R07_PAYMENT_WINDOW_DAYS))
           .groupBy("extract_date", "LIFNR", "BUKRS")
           .agg(F.sum("amount").alias("amount_due"), F.min("NETDT").alias("earliest_due"),
                F.count("*").alias("items_due")))
    hits = (due.join(cc, ["extract_date", "LIFNR", "BUKRS"]).join(v, ["extract_date", "LIFNR"])
            .where((F.col("lfa1_confs") != "") | (F.col("lfb1_confs") != "")))
    detail = F.format_string("Payment will be held: unconfirmed sensitive change; %d open "
                             "item(s) (%s) due by %s", "items_due",
                             F.format_number("amount_due", 2),
                             F.date_format("earliest_due", "yyyy-MM-dd"))
    evidence = ["lfa1_confs", "lfb1_confs", "items_due", "amount_due", "earliest_due"]
    return exceptions(hits, "R07", "Medium", detail, evidence, bukrs=F.col("BUKRS"))


@dp.view(comment="R06 alternative payee or one-time vendor exposure. Reads LFA1-LNRZA, "
                 "LFB1-LNRZB, LFA1-XZEMP, LFA1-XCPDK and vendor payments (ACDOCA BLART KZ). "
                 "No block fields.")
def rule_r06_alternative_payee_one_time():
    v = read("silver_vendor").select("extract_date", "LIFNR", "LNRZA", "XZEMP", "XCPDK")
    cc = read("silver_vendor_company_code").where(F.col("LNRZB") != "").select(
        "extract_date", "LIFNR", "BUKRS", "LNRZB")
    lfa1_hits = v.where((F.col("LNRZA") != "") | (F.col("XZEMP") != "")).select(
        "extract_date", "LIFNR", F.lit("").alias("BUKRS"), F.col("LNRZA").alias("payee"),
        F.when(F.col("LNRZA") != "", F.concat(F.lit("Alternative payee "), "LNRZA",
                                              F.lit(" (LFA1-LNRZA)")))
        .otherwise(F.lit("Payee in document allowed (LFA1-XZEMP)")).alias("reason"))
    lfb1_hits = cc.select("extract_date", "LIFNR", "BUKRS", F.col("LNRZB").alias("payee"),
                          F.concat(F.lit("Alternative payee "), "LNRZB",
                                   F.lit(" in company code "), "BUKRS",
                                   F.lit(" (LFB1-LNRZB)")).alias("reason"))
    # one-time (XCPDK) accounts paid repeatedly look like a real supplier kept off the master
    pays = read("silver_payments").select("LIFNR", "BUDAT", "BELNR", "first_extract_date")
    repeat = (extract_dates().join(pays, (F.col("first_extract_date") <= F.col("extract_date"))
                                   & (F.col("BUDAT") > F.date_sub("extract_date",
                                                                  R01_RECENT_PAYMENT_DAYS)))
              .groupBy("extract_date", "LIFNR").agg(F.countDistinct("BELNR").alias("payments"))
              .where(F.col("payments") >= 2)
              .join(v.where(F.col("XCPDK") != ""), ["extract_date", "LIFNR"])
              .select("extract_date", "LIFNR", F.lit("").alias("BUKRS"),
                      F.lit("").alias("payee"),
                      F.format_string("One-time vendor paid %d times in 90 days (LFA1-XCPDK)",
                                      "payments").alias("reason")))
    hits = lfa1_hits.unionByName(lfb1_hits).unionByName(repeat)
    return exceptions(hits, "R06", "Medium", F.col("reason"), ["payee", "reason"],
                      bukrs=F.col("BUKRS"), related=F.col("payee"))


@dp.materialized_view(comment="Matcher output: candidate duplicate vendor pairs with scores. "
                              "above_threshold marks pairs at or above the tuned threshold")
@dp.expect_or_fail("pair_ordered", "LIFNR_A < LIFNR_B")
def gold_duplicate_candidates():
    names = read("silver_vendor").select("extract_date", "LIFNR", "NAME1")
    latest = names.agg(F.max("extract_date").alias("extract_date"))
    names = names.join(latest, "extract_date").drop("extract_date")
    cand = spark.read.parquet(MATCHER_OUTPUT)  # noqa: F821
    return (cand.join(names.withColumnsRenamed({"LIFNR": "LIFNR_A", "NAME1": "NAME1_A"}),
                      "LIFNR_A", "left")
            .join(names.withColumnsRenamed({"LIFNR": "LIFNR_B", "NAME1": "NAME1_B"}),
                  "LIFNR_B", "left"))


@dp.view(comment="R01 duplicate vendor. Matcher pairs at or above threshold where both vendors "
                 "are active (LFA1 LOEVM deletion flag and SPERR posting block not set) and "
                 "have open items or a payment in the last 90 days.")
def rule_r01_duplicate_vendor():
    v = read("silver_vendor").where((F.col("LOEVM") == "") & (F.col("SPERR") == ""))
    open_items = read("silver_open_items").select("extract_date", "LIFNR").distinct()
    payments = read("silver_payments").select("LIFNR", "BUDAT", "first_extract_date")
    # only payments delivered by that date, so later extracts can't change past results
    paid = (extract_dates().join(payments,
                                 (F.col("first_extract_date") <= F.col("extract_date"))
                                 & (F.col("BUDAT") <= F.col("extract_date"))
                                 & (F.col("BUDAT") > F.date_sub("extract_date",
                                                                R01_RECENT_PAYMENT_DAYS)))
            .select("extract_date", "LIFNR").distinct())
    active = (v.select("extract_date", "LIFNR", "NAME1")
              .join(open_items.unionByName(paid).distinct(), ["extract_date", "LIFNR"]))
    cand = read("gold_duplicate_candidates").where("above_threshold").select(
        "LIFNR_A", "LIFNR_B", "score", "name_score", "address_score")
    a = active.withColumnsRenamed({"LIFNR": "LIFNR_A", "NAME1": "NAME1_A"})
    b = active.withColumnsRenamed({"LIFNR": "LIFNR_B", "NAME1": "NAME1_B"})
    both = cand.join(a, "LIFNR_A").join(b, ["extract_date", "LIFNR_B"])
    # One exception per vendor in the pair, each pointing at the other.
    sides = [both.select("extract_date", F.col("LIFNR_A").alias("LIFNR"),
                         F.col("LIFNR_B").alias("other"), F.col("NAME1_B").alias("other_name"),
                         "score", "name_score", "address_score"),
             both.select("extract_date", F.col("LIFNR_B").alias("LIFNR"),
                         F.col("LIFNR_A").alias("other"), F.col("NAME1_A").alias("other_name"),
                         "score", "name_score", "address_score")]
    hits = sides[0].unionByName(sides[1])
    detail = F.format_string("Possible duplicate of %s (%s), match score %.1f", "other",
                             "other_name", "score")
    evidence = ["other", "other_name", "score", "name_score", "address_score"]
    return exceptions(hits, "R01", "Medium", detail, evidence, related=F.col("other"))


RULE_VIEWS = ["rule_r01_duplicate_vendor", "rule_r02_change_pay_revert", "rule_r03_shared_bank",
              "rule_r04_dormant_not_blocked", "rule_r05_duplicate_invoice_check_off",
              "rule_r06_alternative_payee_one_time", "rule_r07_unconfirmed_change_items_due",
              "rule_r08_payment_after_bank_change"]


@dp.materialized_view(comment="One row per rule hit per extract_date")
@dp.expect_or_fail("lifnr_not_null", "LIFNR IS NOT NULL")
@dp.expect("known_severity", "severity IN ('High', 'Medium', 'Low')")
def gold_exceptions():
    df = read(RULE_VIEWS[0])
    for name in RULE_VIEWS[1:]:
        df = df.unionByName(read(name))
    return df


@dp.materialized_view(comment="Vendors with exceptions ranked by risk = open exposure x max "
                              "severity tier (High 3, Medium 2, Low 1), per extract_date")
def gold_vendor_risk():
    tier = F.create_map(*[x for k, v in SEVERITY_TIER.items() for x in (F.lit(k), F.lit(v))])
    exc = (read("gold_exceptions")
           .groupBy("extract_date", "LIFNR")
           .agg(F.max(F.try_element_at(tier, F.col("severity"))).alias("severity_tier"),
                F.array_sort(F.collect_set("rule_id")).alias("rules"),
                F.count("*").alias("exception_count")))
    exposure = (read("silver_open_items").groupBy("extract_date", "LIFNR")
                .agg(F.sum("amount").alias("exposure")))
    names = read("silver_vendor").select("extract_date", "LIFNR", "NAME1")
    df = (exc.join(exposure, ["extract_date", "LIFNR"], "left")
          .join(names, ["extract_date", "LIFNR"], "left")
          .withColumn("exposure", F.coalesce("exposure", F.lit(0)).cast("decimal(23,2)"))
          .withColumn("risk", (F.col("exposure") * F.col("severity_tier")).cast("decimal(25,2)")))
    rank = Window.partitionBy("extract_date").orderBy(F.desc("risk"), F.desc("severity_tier"),
                                                      "LIFNR")
    return df.withColumn("risk_rank", F.row_number().over(rank))


@dp.materialized_view(comment="Flag counts and flagged exposure per rule per extract_date")
def gold_run_history():
    exc = read("gold_exceptions")
    exposure = (read("silver_open_items").groupBy("extract_date", "LIFNR")
                .agg(F.sum("amount").alias("exposure")))
    per_vendor = exc.select("extract_date", "rule_id", "LIFNR").distinct()
    prev = per_vendor.select(F.date_add("extract_date", 1).alias("extract_date"), "rule_id",
                             "LIFNR", F.lit(True).alias("seen_before"))
    # "new" compares with the previous calendar day (weekends have no postings)
    vendors = (per_vendor.join(prev, ["extract_date", "rule_id", "LIFNR"], "left")
               .join(exposure, ["extract_date", "LIFNR"], "left")
               .groupBy("extract_date", "rule_id")
               .agg(F.count("*").alias("vendors_flagged"),
                    F.count_if(F.col("seen_before").isNull()).alias("new_vendors"),
                    F.sum(F.coalesce("exposure", F.lit(0))).cast("decimal(23,2)")
                    .alias("exposure_flagged")))
    hits = exc.groupBy("extract_date", "rule_id").agg(F.count("*").alias("exceptions"))
    return hits.join(vendors, ["extract_date", "rule_id"])
