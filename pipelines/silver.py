"""Silver: conformed, typed tables. Materialized views, so they are pure functions of bronze.

Expectation policy:
  fail - a broken key would corrupt every join downstream, so stop the update
  drop - the row cannot be used by any rule (no amount, no account number)
  warn - unexpected but usable; keep the row and count it in the event log
"""

from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F

TERM_DAYS = {"NT30": 30, "NT45": 45, "NT60": 60, "0001": 0}


def dats(c: str) -> Column:
    """SAP DATS (YYYYMMDD, blank when empty) to date."""
    return F.when(F.col(c) != "", F.to_date(F.col(c), "yyyyMMdd"))


def read(name: str):
    return spark.read.table(name)  # noqa: F821


@dp.materialized_view(comment="Vendor general data (LFA1) with its Business Partner (via CVI) "
                              "and address, one row per vendor per extract_date")
@dp.expect_or_fail("lifnr_not_null", "LIFNR IS NOT NULL")
@dp.expect("has_business_partner", "PARTNER IS NOT NULL")
@dp.expect("known_account_group", "KTOKK IN ('KRED', 'ZEMP')")
@dp.expect_all({f"{c.lower()}_is_flag": f"{c} IN ('', 'X')"
                for c in ["SPERR", "SPERZ", "SPERM", "LOEVM"]})
@dp.expect("confs_known", "CONFS IN ('', '1', '2')")
def silver_vendor():
    a = read("bronze_lfa1").alias("a")
    c = read("bronze_cvi_vend_link").alias("c")
    b = read("bronze_but000").alias("b")
    r = read("bronze_adrc").alias("r")
    same_day = lambda x, y: F.col(f"{x}.extract_date") == F.col(f"{y}.extract_date")  # noqa: E731
    return (
        a.join(c, (F.col("c.VENDOR") == F.col("a.LIFNR")) & same_day("c", "a"), "left")
        .join(b, (F.col("b.PARTNER_GUID") == F.col("c.PARTNER_GUID")) & same_day("b", "a"), "left")
        .join(r, (F.col("r.ADDRNUMBER") == F.col("a.ADRNR")) & same_day("r", "a"), "left")
        .select(
            "a.extract_date", "a.LIFNR", "b.PARTNER", "a.KTOKK", "a.NAME1", "b.NAME_ORG1",
            (F.col("a.KTOKK") == "ZEMP").alias("is_employee"),
            "r.STREET", "r.PO_BOX", "r.CITY1", "r.POST_CODE1", "r.REGION", "r.COUNTRY",
            "a.STCD2", "a.SPERR", "a.SPERZ", "a.SPERM", "a.LOEVM", "a.XCPDK", "a.LNRZA",
            "a.XZEMP", "a.CONFS", dats("a.ERDAT").alias("created_on"),
        )
    )


@dp.materialized_view(comment="Vendor company code data (LFB1), one row per vendor, company "
                              "code and extract_date")
@dp.expect_all_or_fail({"lifnr_not_null": "LIFNR IS NOT NULL",
                        "bukrs_not_null": "BUKRS IS NOT NULL"})
@dp.expect("known_company_code", "BUKRS IN ('1000', '2000', '3000')")
@dp.expect("known_payment_terms", "term_days IS NOT NULL")
@dp.expect("reprf_is_flag", "REPRF IN ('', 'X')")
def silver_vendor_company_code():
    terms = F.create_map(*[x for k, v in TERM_DAYS.items() for x in (F.lit(k), F.lit(v))])
    return read("bronze_lfb1").select(
        "extract_date", "LIFNR", "BUKRS", "AKONT", "ZTERM",
        F.try_element_at(terms, F.col("ZTERM")).alias("term_days"),
        "ZWELS", "ZAHLS", "SPERR", "LOEVM", "REPRF", "LNRZB", "PERNR", "CONFS",
        dats("ERDAT").alias("created_on"),
    )


@dp.materialized_view(comment="Vendor bank details (LFBK) as of each extract_date")
@dp.expect_all_or_drop({"bank_key_present": "BANKL != '' AND BANKN != ''"})
@dp.expect("us_bank_country", "BANKS = 'US'")
@dp.expect("routing_number_9_digits", "BANKL RLIKE '^[0-9]{9}$'")
def silver_vendor_bank():
    return read("bronze_lfbk").select("extract_date", "LIFNR", "BANKS", "BANKL", "BANKN",
                                      "KOINH", "BKVID")


@dp.materialized_view(comment="Business Partner bank details (BUT0BK) with validity. "
                              "is_valid is true only for the row in force at end of extract_date")
@dp.expect_all_or_fail({"partner_not_null": "PARTNER IS NOT NULL",
                        "bkvid_not_null": "BKVID IS NOT NULL"})
@dp.expect_or_drop("validity_ordered", "valid_from <= valid_to")
def silver_bp_bank():
    ts = lambda c: F.to_timestamp(F.col(c), "yyyyMMddHHmmss")  # noqa: E731
    end_of_day = F.to_timestamp(F.concat(F.date_format("extract_date", "yyyyMMdd"),
                                         F.lit("235959")), "yyyyMMddHHmmss")
    df = read("bronze_but0bk").select(
        "extract_date", "PARTNER", "BKVID", "BANKS", "BANKL", "BANKN", "IBAN", "KOINH",
        ts("BK_VALID_FROM").alias("valid_from"), ts("BK_VALID_TO").alias("valid_to"))
    return (df.withColumn("is_valid", (F.col("valid_from") <= end_of_day)
                          & (F.col("valid_to") >= end_of_day))
            .withColumn("is_future_dated", F.col("valid_from") > end_of_day))


@dp.materialized_view(comment="Addresses (ADRC) as of each extract_date")
@dp.expect_or_fail("addrnumber_not_null", "ADDRNUMBER IS NOT NULL")
@dp.expect("us_address", "COUNTRY = 'US'")
@dp.expect("zip_5_digits_or_blank", "POST_CODE1 = '' OR POST_CODE1 RLIKE '^[0-9]{5}$'")
def silver_address():
    return read("bronze_adrc").select("extract_date", "ADDRNUMBER", "NAME1", "STREET", "PO_BOX",
                                      "CITY1", "POST_CODE1", "REGION", "COUNTRY")


@dp.materialized_view(comment="Vendor lines from the universal journal: leading ledger 0L and "
                              "KOART K only. One row per line, latest version; "
                              "cleared_extract_date is when AUGBL first appeared")
@dp.expect_all_or_fail({"line_key_not_null":
                        "RBUKRS IS NOT NULL AND GJAHR IS NOT NULL AND BELNR IS NOT NULL "
                        "AND DOCLN IS NOT NULL"})
@dp.expect_all_or_drop({"amount_present": "HSL IS NOT NULL",
                        "vendor_present": "LIFNR != ''",
                        "known_doc_type": "BLART IN ('KR', 'KZ')"})
@dp.expect("invoice_has_due_date", "BLART != 'KR' OR NETDT IS NOT NULL")
def silver_journal_lines():
    # Without these two filters every line counts once per ledger, plus the offset lines.
    lines = read("bronze_acdoca").where((F.col("RLDNR") == "0L") & (F.col("KOART") == "K"))
    latest = lambda c: F.max_by(c, "extract_date").alias(c)  # noqa: E731
    return (
        lines.groupBy("RBUKRS", "GJAHR", "BELNR", "DOCLN")
        .agg(latest("LIFNR"), latest("BLART"), latest("BUDAT"), latest("NETDT"), latest("HSL"),
             latest("RHCUR"), latest("AUGBL"), latest("AUGDT"),
             F.min("extract_date").alias("first_extract_date"),
             F.min(F.when(F.col("AUGBL") != "", F.col("extract_date")))
             .alias("cleared_extract_date"))
        .select("RBUKRS", "GJAHR", "BELNR", "DOCLN", "LIFNR", "BLART",
                dats("BUDAT").alias("BUDAT"),
                # NETDT is on ACDOCA in S/4HANA; on older releases derive it from the
                # baseline date plus payment term days.
                dats("NETDT").alias("NETDT"),
                F.col("HSL").cast("decimal(23,2)").alias("HSL"), "RHCUR", "AUGBL",
                dats("AUGDT").alias("AUGDT"), "first_extract_date", "cleared_extract_date")
    )


@dp.materialized_view(comment="Open vendor items as of each extract_date (AUGBL blank then)")
@dp.expect("open_amount_nonzero", "HSL != 0")
def silver_open_items():
    dates = read("bronze_lfa1").select("extract_date").distinct()
    lines = read("silver_journal_lines")
    is_open = (F.col("first_extract_date") <= F.col("extract_date")) & (
        F.col("cleared_extract_date").isNull()
        | (F.col("cleared_extract_date") > F.col("extract_date")))
    return (dates.crossJoin(lines).where(is_open)
            .select("extract_date", "LIFNR", F.col("RBUKRS").alias("BUKRS"), "GJAHR", "BELNR",
                    "DOCLN", "BLART", "BUDAT", "NETDT", "HSL", F.abs("HSL").alias("amount")))


@dp.materialized_view(comment="Vendor payments (BLART KZ), one row per payment document")
def silver_payments():
    return (read("silver_journal_lines").where(F.col("BLART") == "KZ")
            .select("LIFNR", F.col("RBUKRS").alias("BUKRS"), "GJAHR", "BELNR", "BUDAT", "HSL",
                    "first_extract_date"))
