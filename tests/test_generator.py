from dataclasses import replace
from datetime import timedelta

import pandas as pd
import pytest

from generator.config import Config
from generator.simulate import Simulator

N_VENDORS = 2000


@pytest.fixture(scope="module")
def vendors():
    return pd.read_parquet("data/vendor_master.parquet").head(N_VENDORS)


@pytest.fixture(scope="module")
def cfg():
    return Config()


@pytest.fixture(scope="module")
def out(vendors, cfg):
    return Simulator(vendors.copy(), cfg).run()


def stacked(out, table):
    return pd.concat([df.assign(extract_date=d.strftime("%Y%m%d"))
                      for d, t in out.extracts.items() for df in [t[table]]], ignore_index=True)


def latest_ledger(out):
    acd = stacked(out, "ACDOCA")
    acd = acd.sort_values("extract_date").drop_duplicates(
        ["RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN"], keep="last")
    acd["HSL"] = acd.HSL.astype(float)
    return acd


def expected(out, rule):
    m = out.manifest
    return set(m[(m.rule_id == rule) & m.expected_flag].LIFNR)


def test_same_seed_same_output(vendors, cfg, out):
    again = Simulator(vendors.copy(), cfg).run()
    for d, tables in out.extracts.items():
        for name, df in tables.items():
            pd.testing.assert_frame_equal(df, again.extracts[d][name], obj=f"{d} {name}")
    pd.testing.assert_frame_equal(out.manifest, again.manifest)


def test_different_seed_changes_output(vendors, cfg, out):
    other = Simulator(vendors.copy(), replace(cfg, seed=7)).run()
    d = cfg.start
    assert not out.extracts[d]["LFBK"].equals(other.extracts[d]["LFBK"])


def test_row_counts(out, cfg):
    n_all = N_VENDORS + cfg.employees
    for d, t in out.extracts.items():
        assert len(t["LFA1"]) == n_all
        assert len(t["LFBK"]) == n_all  # one current bank row per vendor
        assert len(t["DFKKBPTAXNUM"]) == N_VENDORS
        assert len(t["CVI_VEND_LINK"]) == n_all
        acd = t["ACDOCA"]
        assert (acd.RLDNR == "0L").sum() == (acd.RLDNR == "2L").sum()
        if d.weekday() >= 5:
            assert acd.empty
    assert out.row_counts.shape == (11, cfg.days)


def test_injected_counts_match_rates(out, cfg):
    r = cfg.rates
    counts = out.manifest[out.manifest.expected_flag].groupby("rule_id").size()
    assert counts["R02"] == round(r.r02_change_pay_revert * N_VENDORS)
    assert counts["R03"] == 2 * (round(r.r03_shared_vendor_pairs * N_VENDORS)
                                 + round(r.r03_employee_shares * N_VENDORS))
    assert counts["R04"] == (round(r.r04_dormant_unblocked * N_VENDORS)
                             + round(r.r04_dormant_sperm_only * N_VENDORS))
    assert counts["R05"] == round(r.r05_reprf_blank * N_VENDORS)
    assert counts["R07"] == round(r.r07_unconfirmed_open_items * N_VENDORS)


def test_ledger_balances_and_clears(out):
    acd = latest_ledger(out)
    assert (acd.groupby(["RLDNR", "RBUKRS", "GJAHR", "BELNR"]).HSL.sum().round(2) == 0).all()
    k = acd[(acd.RLDNR == "0L") & (acd.KOART == "K")]
    cleared = k[(k.BLART == "KR") & (k.AUGBL != "")]
    pay = k[k.BLART == "KZ"].set_index("BELNR")
    assert cleared.AUGBL.isin(pay.index).all()
    assert (cleared.AUGDT >= cleared.BUDAT).all()
    sums = cleared.groupby("AUGBL").HSL.sum().round(2)
    assert (pay.loc[sums.index].HSL.round(2) == -sums).all()


def test_baseline_is_clean_for_static_rules(out, cfg):
    end = cfg.start + timedelta(days=cfg.days - 1)
    t = out.extracts[end]
    lfa1, lfb1, lfbk = t["LFA1"], t["LFB1"], t["LFBK"]
    k = latest_ledger(out)
    k = k[(k.RLDNR == "0L") & (k.KOART == "K")]

    assert set(lfb1[lfb1.REPRF == ""].LIFNR) == expected(out, "R05")

    shared = lfbk.groupby(["BANKS", "BANKL", "BANKN"]).LIFNR.transform("nunique") > 1
    assert set(lfbk[shared].LIFNR) == expected(out, "R03")

    last_post = k.groupby("LIFNR").BUDAT.max()
    cut = (end - timedelta(days=548)).strftime("%Y%m%d")
    unblocked = set(lfa1[(lfa1.SPERR == "") & (lfa1.SPERZ == "") & (lfa1.LOEVM == "")].LIFNR)
    assert set(last_post[last_post < cut].index) & unblocked == expected(out, "R04")

    due = (end + timedelta(days=7)).strftime("%Y%m%d")
    inv = k[(k.BLART == "KR") & (k.AUGBL == "") & (k.NETDT <= due)]
    assert set(lfa1[lfa1.CONFS != ""].LIFNR) & set(inv.LIFNR) == expected(out, "R07")


def test_r02_pattern_in_change_documents(out):
    hdr, pos = stacked(out, "CDHDR"), stacked(out, "CDPOS")
    ins = pos[(pos.TABNAME == "LFBK") & (pos.CHNGIND == "I")].merge(
        hdr[["CHANGENR", "UDATE"]], on="CHANGENR")
    k = latest_ledger(out)
    pays = k[(k.RLDNR == "0L") & (k.KOART == "K") & (k.BLART == "KZ")]
    for lifnr in expected(out, "R02"):
        dates = sorted(ins[ins.OBJECTID == lifnr].UDATE)
        assert len(dates) == 2
        paid = pays[pays.LIFNR == lifnr].BUDAT
        assert ((paid >= dates[0]) & (paid <= dates[1])).any()


def test_future_dated_bank_rows_not_on_vendor_yet(out):
    for d, t in out.extracts.items():
        bk = t["BUT0BK"]
        future = bk[bk.BK_VALID_FROM > d.strftime("%Y%m%d") + "235959"]
        if future.empty:
            continue
        on_vendor = t["LFBK"].merge(future, on=["BANKS", "BANKL", "BANKN"])
        assert on_vendor.empty
        return
    pytest.fail("no future-dated BUT0BK rows generated")


def test_identifiers_are_obviously_fake(out, cfg):
    t = out.extracts[cfg.start]
    assert t["LFBK"].BANKL.str.startswith("000").all()
    assert t["LFBK"].BANKN.str.startswith("9990").all()
    assert t["DFKKBPTAXNUM"].TAXNUM.str.startswith("00-").all()
    assert "uei" not in out.key_map.columns
