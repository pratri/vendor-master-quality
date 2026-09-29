"""Seeded synthetic SAP vendor data and daily extract replay.

Master data is a full snapshot per extract_date. ACDOCA is a delta: lines posted that day,
plus earlier lines that were cleared that day (re-sent with AUGBL filled). Day 1 also carries
the history needed for the dormancy rule. CDHDR/CDPOS carry that day's change documents.

Every control exception comes from a planned injection, recorded in the manifest.
The baseline is kept clean so detections can be reconciled one to one.
"""

import uuid
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd

from generator.config import Config

MANDT = "100"
COMPANY_CODES = ("1000", "2000", "3000")
RECON_ACCOUNT = "0000211000"
EXPENSE_ACCOUNT = "0000610000"
BANK_CLEARING_ACCOUNT = "0000113100"
# Payment terms are customer config (T052); these keys are typical, not SAP-delivered.
TERMS = {"NT30": 30, "NT45": 45, "NT60": 60, "0001": 0}
CLERKS = [f"APCLERK{i:02d}" for i in range(1, 9)]
APPROVERS = [f"APSUPER{i:02d}" for i in range(1, 4)]
CVI_USER = "CVI_SYNC"
PAY_RUN_WEEKDAYS = (1, 4)  # Tuesday, Friday
PAY_RUN_TIME = time(12, 0)
PAY_AHEAD_DAYS = 3  # a run pays items due up to 3 days after it
OPEN_TO = "99991231235959"

MASTER_TABLES = ["LFA1", "LFB1", "LFBK", "BUT000", "BUT0BK", "ADRC", "DFKKBPTAXNUM",
                 "CVI_VEND_LINK"]
DELTA_TABLES = ["ACDOCA", "CDHDR", "CDPOS"]

LFA1_COLS = ["LIFNR", "KTOKK", "NAME1", "ADRNR", "LAND1", "STCD1", "STCD2", "SPERR", "SPERZ",
             "SPERM", "LOEVM", "XCPDK", "LNRZA", "XZEMP", "CONFS", "ERDAT"]
LFB1_COLS = ["LIFNR", "BUKRS", "AKONT", "ZTERM", "ZWELS", "ZAHLS", "SPERR", "LOEVM", "REPRF",
             "LNRZB", "PERNR", "CONFS", "ERDAT"]
LFBK_COLS = ["LIFNR", "BANKS", "BANKL", "BANKN", "KOINH", "BVTYP"]
BUT0BK_COLS = ["PARTNER", "BKVID", "BANKS", "BANKL", "BANKN", "IBAN", "KOINH", "BK_VALID_FROM",
               "BK_VALID_TO"]
ACDOCA_COLS = ["RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN", "KOART", "RACCT", "LIFNR", "BLART",
               "BUDAT", "HSL", "RHCUR", "AUGBL", "AUGDT", "NETDT"]
CDHDR_COLS = ["OBJECTCLAS", "OBJECTID", "CHANGENR", "USERNAME", "UDATE", "UTIME", "TCODE",
              "CHANGE_IND"]
CDPOS_COLS = ["OBJECTCLAS", "OBJECTID", "CHANGENR", "TABNAME", "TABKEY", "FNAME", "CHNGIND",
              "VALUE_NEW", "VALUE_OLD"]


def dats(d: date) -> str:
    return d.strftime("%Y%m%d")


def tims(t: datetime) -> str:
    return t.strftime("%H%M%S")


def tstmp(t: datetime) -> str:
    return t.strftime("%Y%m%d%H%M%S")


def tabkey(tab: str, key: tuple) -> str:
    """Fixed-width CDPOS TABKEY: client plus the table's key fields."""
    widths = {"LFA1": (10,), "LFB1": (10, 4), "LFBK": (10, 3, 15, 18), "BUT0BK": (10, 4)}[tab]
    return MANDT + "".join(str(k).ljust(w) for k, w in zip(key, widths, strict=True))


@dataclass
class Item:
    tab: str
    key: tuple
    fname: str
    chngind: str  # U update, I insert, D delete
    old: str = ""
    new: str = ""
    payload: dict | None = None  # full row for inserts


@dataclass
class Change:
    ts: datetime
    objectid: str
    user: str
    tcode: str
    items: list[Item]
    objectclas: str = "KRED"
    changenr: str = ""


@dataclass
class Output:
    extracts: dict[date, dict[str, pd.DataFrame]]
    manifest: pd.DataFrame
    key_map: pd.DataFrame
    row_counts: pd.DataFrame = field(default_factory=pd.DataFrame)


class Simulator:
    def __init__(self, vendors: pd.DataFrame, cfg: Config):
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.days = [cfg.start + timedelta(i) for i in range(cfg.days)]
        self.end = self.days[-1]
        self.weekdays = [d for d in self.days if d.weekday() < 5]
        self.runs_window = [d for d in self.days if d.weekday() in PAY_RUN_WEEKDAYS]
        # Day 1 is the initial load, so master data changes start on day 2.
        self.event_days = [d for d in self.weekdays if d > cfg.start]
        self.event_runs = [d for d in self.runs_window if d > cfg.start]
        self.vendors = vendors.sort_values("vendor_id").reset_index(drop=True)
        self.manifest: list[dict] = []
        self.changes: list[Change] = []
        self.explicit_invoices: list[dict] = []
        self.mode: dict[str, str] = {}  # history profile per vendor
        self.cur_bank: dict[str, tuple] = {}  # planning-time bank per vendor
        self.next_bkvid: dict[str, int] = {}
        self._bank_pool = iter(self.rng.permutation(900_000) + 100_000)

    # ---------- keys and base master data ----------

    def _new_bank(self) -> tuple:
        n = next(self._bank_pool)
        # Obviously fake: routing numbers never start with 000, accounts use a 9990 prefix.
        return ("US", f"000{n:06d}", f"9990{n:06d}")

    def build_master(self) -> None:
        v = self.vendors
        n = len(v)
        rng = self.rng
        idx = np.arange(1, n + 1)
        v["LIFNR"] = [f"{100000 + i:010d}" for i in idx]
        v["PARTNER"] = [f"{2000000 + i:010d}" for i in idx]
        v["ADRNR"] = [f"{3000000 + i:010d}" for i in idx]
        self.key_map = v[["vendor_id", "LIFNR", "PARTNER"]].copy()
        ein = rng.permutation(10_000_000)[:n]

        lfa1, lfb1, adrc, but000, taxnum, cvi = [], [], [], [], [], []
        erdat = pd.Timestamp(self.cfg.start) - pd.to_timedelta(rng.integers(700, 6500, n),
                                                               unit="D")
        for i, r in enumerate(v.itertuples(index=False)):
            guid = uuid.uuid5(uuid.NAMESPACE_OID, r.PARTNER).hex.upper()
            lfa1.append(dict(LIFNR=r.LIFNR, KTOKK="KRED", NAME1=r.name_raw[:35], ADRNR=r.ADRNR,
                             LAND1="US", STCD1="", STCD2=f"00-{ein[i]:07d}", SPERR="", SPERZ="",
                             SPERM="", LOEVM="", XCPDK="", LNRZA="", XZEMP="", CONFS="",
                             ERDAT=dats(erdat[i].date())))
            is_po = r.address_norm.startswith("PO BOX ")
            adrc.append(dict(ADDRNUMBER=r.ADRNR, DATE_FROM="00010101", NATION="",
                             NAME1=r.name_raw[:40],
                             STREET="" if is_po else r.address_line_1.upper()[:60],
                             CITY1=r.city.upper(), POST_CODE1=r.zip5, REGION=r.state,
                             COUNTRY="US", PO_BOX=r.address_norm[7:] if is_po else ""))
            but000.append(dict(PARTNER=r.PARTNER, PARTNER_GUID=guid, TYPE="2",
                               NAME_ORG1=r.name_raw[:40], NAME_FIRST="", NAME_LAST=""))
            # US2 is the EIN tax type; 00- prefix is never issued by the IRS.
            taxnum.append(dict(PARTNER=r.PARTNER, TAXTYPE="US2", TAXNUM=f"00-{ein[i]:07d}"))
            cvi.append(dict(PARTNER_GUID=guid, VENDOR=r.LIFNR))
            ccs = ["1000"]
            if rng.random() < self.cfg.second_cc_share:
                ccs.append("2000")
            if rng.random() < self.cfg.third_cc_share:
                ccs.append("3000")
            for cc in ccs:
                lfb1.append(dict(LIFNR=r.LIFNR, BUKRS=cc, AKONT=RECON_ACCOUNT,
                                 ZTERM=rng.choice(["NT30", "NT45", "NT60"], p=[0.6, 0.25, 0.15]),
                                 ZWELS=rng.choice(["T", "C"], p=[0.8, 0.2]), ZAHLS="", SPERR="",
                                 LOEVM="", REPRF="X", LNRZB="", PERNR="", CONFS="",
                                 ERDAT=dats(erdat[i].date())))

        # Employee vendors for travel expenses; PERNR links them to HR.
        emp_keys = []
        for j in range(1, self.cfg.employees + 1):
            lifnr, partner = f"{900000 + j:010d}", f"{9000000 + j:010d}"
            guid = uuid.uuid5(uuid.NAMESPACE_OID, partner).hex.upper()
            name = f"EMPLOYEE {j:05d}"
            src = v.iloc[int(rng.integers(n))]
            lfa1.append(dict(LIFNR=lifnr, KTOKK="ZEMP", NAME1=name, ADRNR=f"{3900000 + j:010d}",
                             LAND1="US", STCD1="", STCD2="", SPERR="", SPERZ="", SPERM="",
                             LOEVM="", XCPDK="", LNRZA="", XZEMP="", CONFS="",
                             ERDAT=dats(self.cfg.start - timedelta(int(rng.integers(200, 4000))))))
            adrc.append(dict(ADDRNUMBER=f"{3900000 + j:010d}", DATE_FROM="00010101", NATION="",
                             NAME1=name, STREET="", CITY1=src.city.upper(), POST_CODE1=src.zip5,
                             REGION=src.state, COUNTRY="US", PO_BOX=""))
            but000.append(dict(PARTNER=partner, PARTNER_GUID=guid, TYPE="1", NAME_ORG1="",
                               NAME_FIRST="EMPLOYEE", NAME_LAST=f"{j:05d}"))
            cvi.append(dict(PARTNER_GUID=guid, VENDOR=lifnr))
            lfb1.append(dict(LIFNR=lifnr, BUKRS="1000", AKONT=RECON_ACCOUNT, ZTERM="0001",
                             ZWELS="T", ZAHLS="", SPERR="", LOEVM="", REPRF="X", LNRZB="",
                             PERNR=f"{10000 + j:08d}", CONFS="", ERDAT=lfa1[-1]["ERDAT"]))
            emp_keys.append({"vendor_id": "", "LIFNR": lifnr, "PARTNER": partner})
        # One-time vendor accounts (account group CPD): name and address come with each
        # document, so there is no bank master and no USAspending counterpart.
        n_cpd = self.cfg.one_time_repeat + self.cfg.one_time_single
        for j in range(1, n_cpd + 1):
            lifnr, partner = f"{800000 + j:010d}", f"{8000000 + j:010d}"
            guid = uuid.uuid5(uuid.NAMESPACE_OID, partner).hex.upper()
            name = f"ONE-TIME VENDOR {j:02d}"
            erdat = dats(self.cfg.start - timedelta(days=30))
            lfa1.append(dict(LIFNR=lifnr, KTOKK="CPD", NAME1=name, ADRNR=f"{3800000 + j:010d}",
                             LAND1="US", STCD1="", STCD2="", SPERR="", SPERZ="", SPERM="",
                             LOEVM="", XCPDK="X", LNRZA="", XZEMP="", CONFS="", ERDAT=erdat))
            adrc.append(dict(ADDRNUMBER=f"{3800000 + j:010d}", DATE_FROM="00010101", NATION="",
                             NAME1=name, STREET="", CITY1="", POST_CODE1="", REGION="",
                             COUNTRY="US", PO_BOX=""))
            but000.append(dict(PARTNER=partner, PARTNER_GUID=guid, TYPE="2", NAME_ORG1=name,
                               NAME_FIRST="", NAME_LAST=""))
            cvi.append(dict(PARTNER_GUID=guid, VENDOR=lifnr))
            lfb1.append(dict(LIFNR=lifnr, BUKRS="1000", AKONT=RECON_ACCOUNT, ZTERM="0001",
                             ZWELS="C", ZAHLS="", SPERR="", LOEVM="", REPRF="X", LNRZB="",
                             PERNR="", CONFS="", ERDAT=erdat))
            emp_keys.append({"vendor_id": "", "LIFNR": lifnr, "PARTNER": partner})
        self.key_map = pd.concat([self.key_map, pd.DataFrame(emp_keys)], ignore_index=True)

        self.lfa1 = {r["LIFNR"]: r for r in lfa1}
        self.lfb1 = {(r["LIFNR"], r["BUKRS"]): r for r in lfb1}
        self.adrc, self.but000, self.taxnum, self.cvi = adrc, but000, taxnum, cvi
        self.partner_of = dict(zip(self.key_map["LIFNR"], self.key_map["PARTNER"], strict=True))
        self.lfbk, self.but0bk, self.lfbk_key = {}, {}, {}
        valid_from = "20200101000000"
        for lifnr in self.lfa1:
            if self.lfa1[lifnr]["XCPDK"]:
                continue
            bank = self._new_bank()
            self._set_base_bank(lifnr, bank, valid_from)
        self.vendor_lifnrs = list(v["LIFNR"])
        self.employee_lifnrs = [x for x in self.lfa1 if x.startswith("0000900")]
        self.cpd_lifnrs = [x for x in self.lfa1 if self.lfa1[x]["XCPDK"]]
        self.tx_count = dict(zip(v["LIFNR"], v["transaction_count"], strict=True))

    def _set_base_bank(self, lifnr: str, bank: tuple, valid_from: str) -> None:
        if lifnr in self.lfbk_key:
            del self.lfbk[self.lfbk_key[lifnr]]
        partner = self.partner_of[lifnr]
        koinh = self.lfa1[lifnr]["NAME1"]
        self.lfbk_key[lifnr] = (lifnr, *bank)
        self.lfbk[(lifnr, *bank)] = dict(LIFNR=lifnr, BANKS=bank[0], BANKL=bank[1],
                                          BANKN=bank[2], KOINH=koinh, BVTYP="0001")
        self.but0bk[(partner, "0001")] = dict(PARTNER=partner, BKVID="0001", BANKS=bank[0],
                                              BANKL=bank[1], BANKN=bank[2], IBAN="",
                                              KOINH=koinh, BK_VALID_FROM=valid_from,
                                              BK_VALID_TO=OPEN_TO)
        self.cur_bank[lifnr] = bank
        self.next_bkvid[lifnr] = 2

    # ---------- planned changes ----------

    def _bank_change(self, lifnr: str, ts: datetime, new_bank: tuple, user: str,
                     approve_at: datetime | None, valid_from: datetime | None = None) -> None:
        """BP bank change: BUT0BK on the BP side, then LFBK and CONFS once the row is valid."""
        partner = self.partner_of[lifnr]
        effective = valid_from or ts
        old_bank = self.cur_bank[lifnr]
        old_bkvid = f"{self.next_bkvid[lifnr] - 1:04d}"
        bkvid = f"{self.next_bkvid[lifnr]:04d}"
        self.next_bkvid[lifnr] += 1
        koinh = self.lfa1[lifnr]["NAME1"]
        new_row = dict(PARTNER=partner, BKVID=bkvid, BANKS=new_bank[0], BANKL=new_bank[1],
                       BANKN=new_bank[2], IBAN="", KOINH=koinh, BK_VALID_FROM=tstmp(effective),
                       BK_VALID_TO=OPEN_TO)
        # Object class BUPA_BUP for BP change documents (worth confirming on a real system).
        self.changes.append(Change(ts, partner, user, "BP", [
            Item("BUT0BK", (partner, old_bkvid), "BK_VALID_TO", "U", OPEN_TO,
                 tstmp(effective - timedelta(seconds=1))),
            Item("BUT0BK", (partner, bkvid), "KEY", "I", payload=new_row),
        ], objectclas="BUPA_BUP"))
        # CVI moves the vendor bank when the BP row becomes valid.
        vend_user = user if valid_from is None else CVI_USER
        vend_ts = ts if valid_from is None else valid_from + timedelta(minutes=5)
        self.changes.append(Change(vend_ts, lifnr, vend_user, "BP" if valid_from is None
                                   else "CVI_SYNC", [
            Item("LFBK", (lifnr, *old_bank), "KEY", "D"),
            Item("LFBK", (lifnr, *new_bank), "KEY", "I",
                 payload=dict(LIFNR=lifnr, BANKS=new_bank[0], BANKL=new_bank[1],
                              BANKN=new_bank[2], KOINH=koinh, BVTYP=bkvid)),
            # Bank data is a sensitive field (T055F), so the change needs confirmation.
            Item("LFA1", (lifnr,), "CONFS", "U", "", "1"),
        ]))
        if approve_at is not None:
            approve_at = max(approve_at, vend_ts + timedelta(minutes=10))
            self._confirm(lifnr, approve_at)
        self.cur_bank[lifnr] = new_bank

    def _confirm(self, lifnr: str, ts: datetime) -> None:
        user = APPROVERS[int(self.rng.integers(len(APPROVERS)))]
        self.changes.append(Change(ts, lifnr, user, "FK08",
                                   [Item("LFA1", (lifnr,), "CONFS", "U", "1", "")]))

    def _field_change(self, lifnr: str, ts: datetime, tab: str, key: tuple, fname: str,
                      old: str, new: str) -> None:
        user = CLERKS[int(self.rng.integers(len(CLERKS)))]
        tcode = "FK02" if tab == "LFB1" else "BP"
        self.changes.append(Change(ts, lifnr, user, tcode, [Item(tab, key, fname, "U", old, new)]))

    def _at(self, d: date, h: int, m: int = 0) -> datetime:
        return datetime.combine(d, time(h, m))

    def _clerk(self) -> str:
        return CLERKS[int(self.rng.integers(len(CLERKS)))]

    def _add(self, rule_id: str, variant: str, lifnr: str, expected: bool, bukrs: str = "",
             other: str = "", event_date: date | None = None, detail: str = "") -> None:
        self.manifest.append(dict(rule_id=rule_id, variant=variant, expected_flag=expected,
                                  LIFNR=lifnr, BUKRS=bukrs, OTHER_LIFNR=other,
                                  event_date=dats(event_date) if event_date else "",
                                  detail=detail))

    def _prev_run(self, d: date) -> date:
        """Last payment run strictly before d."""
        x = d - timedelta(days=1)
        while x.weekday() not in PAY_RUN_WEEKDAYS:
            x -= timedelta(days=1)
        return x

    def _explicit_invoice(self, lifnr: str, bukrs: str, netdt: date) -> None:
        term = TERMS[self.lfb1[(lifnr, bukrs)]["ZTERM"]]
        budat = netdt - timedelta(days=term)
        while budat.weekday() >= 5:
            budat -= timedelta(days=1)
        amount = round(float(self.rng.lognormal(np.log(12000), 0.6)), 2)
        self.explicit_invoices.append(dict(LIFNR=lifnr, BUKRS=bukrs, budat=budat, netdt=netdt,
                                           amount=amount))

    def plan(self) -> None:
        r, rng = self.cfg.rates, self.rng
        n = len(self.vendor_lifnrs)
        pool = list(rng.permutation(self.vendor_lifnrs))

        def take(rate: float, k: int = 1) -> list[str]:
            count = round(rate * n) * k
            out = pool[:count]
            del pool[:count]
            return out

        for lifnr in self.vendor_lifnrs:
            self.mode[lifnr] = "recent"
        for lifnr in self.employee_lifnrs:
            self.mode[lifnr] = "recent"
        for lifnr in self.cpd_lifnrs:
            self.mode[lifnr] = "cpd"  # postings only from the explicit invoices below

        # Natural blocks: posting/payment/deletion blocked vendors are old and dormant.
        # Purchasing-block-only vendors stay active, since SPERM alone does not stop R04.
        for lifnr in take(r.blocked_natural):
            kind = rng.choice(["SPERR", "SPERZ", "LOEVM", "SPERM"], p=[0.3, 0.3, 0.25, 0.15])
            self.lfa1[lifnr][kind] = "X"
            if kind == "LOEVM":
                self.lfa1[lifnr]["SPERR"] = "X"
            if kind != "SPERM":
                self.mode[lifnr] = "dormant"
                self._add("R04", f"decoy_blocked_{kind}", lifnr, False,
                          detail=f"dormant but {kind} set")

        # R04 dormant and not blocked (SPERM-only still counts as not blocked).
        for lifnr in take(r.r04_dormant_unblocked):
            self.mode[lifnr] = "dormant"
            self._add("R04", "unblocked", lifnr, True)
        for lifnr in take(r.r04_dormant_sperm_only):
            self.mode[lifnr] = "dormant"
            self.lfa1[lifnr]["SPERM"] = "X"
            self._add("R04", "sperm_only", lifnr, True, detail="only purchasing block set")

        # R03 shared bank accounts, present from the start.
        pair_vendors = take(r.r03_shared_vendor_pairs, k=2)
        for a, b in zip(pair_vendors[::2], pair_vendors[1::2], strict=True):
            self._set_base_bank(b, self.cur_bank[a], "20200101000000")
            self._add("R03", "vendor_pair", a, True, other=b)
            self._add("R03", "vendor_pair", b, True, other=a)
        emp_pool = list(rng.permutation(self.employee_lifnrs))
        for lifnr in take(r.r03_employee_shares):
            emp = emp_pool.pop()
            self._set_base_bank(emp, self.cur_bank[lifnr], "20200101000000")
            self._add("R03", "employee_share", lifnr, True, other=emp)
            self._add("R03", "employee_share", emp, True, other=lifnr, detail="LFB1-PERNR set")

        # R05 duplicate invoice check off, per vendor-company code.
        vendor_set = set(self.vendor_lifnrs)
        vcc = sorted(k for k in self.lfb1 if k[0] in vendor_set)
        n_r05 = round(r.r05_reprf_blank * n)
        for i in rng.permutation(len(vcc))[:n_r05]:
            lifnr, bukrs = vcc[i]
            if rng.random() < r.r05_changed_in_window_share:
                d = self.event_days[int(rng.integers(len(self.event_days)))]
                self._field_change(lifnr, self._at(d, int(rng.integers(8, 17)),
                                                   int(rng.integers(60))),
                                   "LFB1", (lifnr, bukrs), "REPRF", "X", "")
                self._add("R05", "changed_in_window", lifnr, True, bukrs=bukrs, event_date=d)
            else:
                self.lfb1[(lifnr, bukrs)]["REPRF"] = ""
                self._add("R05", "static", lifnr, True, bukrs=bukrs)

        # R02 change, pay, revert. The payment run falls between the two bank changes.
        for lifnr in take(r.r02_change_pay_revert):
            original = self.cur_bank[lifnr]
            if rng.random() < r.r02_same_day_share:
                run = self.event_runs[int(rng.integers(len(self.event_runs)))]
                change_at, revert_at = self._at(run, 8, 15), self._at(run, 16, 45)
                variant = "same_day"
            else:
                run = self.event_runs[int(rng.integers(len(self.event_runs)))]
                prev = self._prev_run(run)
                gap = [d for d in self.event_days if prev < d < run]
                change_day = gap[int(rng.integers(len(gap)))] if gap else prev
                change_at = self._at(change_day, 10)
                revert_day = run + timedelta(days=1)
                while revert_day.weekday() >= 5:
                    revert_day += timedelta(days=1)
                revert_at = self._at(revert_day, 15)
                variant = "multi_day"
            netdt = self._prev_run(run) + timedelta(days=PAY_AHEAD_DAYS + 1)
            self._explicit_invoice(lifnr, "1000", max(netdt, run - timedelta(days=1)))
            user = self._clerk()
            self._bank_change(lifnr, change_at, self._new_bank(), user,
                              change_at + timedelta(minutes=45))
            self._bank_change(lifnr, revert_at, original, user, revert_at + timedelta(minutes=25))
            self._add("R02", variant, lifnr, True, bukrs="1000", event_date=change_at.date(),
                      detail=f"pay {dats(run)} revert {dats(revert_at.date())}")

        # Decoy: change and revert on a non-run day, no payment in between.
        quiet = [d for d in self.event_days if d.weekday() not in PAY_RUN_WEEKDAYS]
        for lifnr in take(r.d02_change_revert_no_payment):
            d = quiet[int(rng.integers(len(quiet)))]
            original = self.cur_bank[lifnr]
            user = self._clerk()
            self._bank_change(lifnr, self._at(d, 9), self._new_bank(), user, self._at(d, 9, 40))
            self._bank_change(lifnr, self._at(d, 14), original, user, self._at(d, 14, 30))
            self._add("R02", "decoy_no_payment", lifnr, False, event_date=d)

        # R07 unconfirmed bank change while an item is due soon. CONFS blocks payment.
        for lifnr in take(r.r07_unconfirmed_open_items):
            d = self.event_days[int(rng.integers(len(self.event_days) - 1))]
            ts = self._at(d, int(rng.integers(8, 11)))
            first_ok = self._prev_run(d) + timedelta(days=PAY_AHEAD_DAYS + 1)
            if d.weekday() in PAY_RUN_WEEKDAYS:  # change before noon beats that day's run
                first_ok = min(first_ok, d + timedelta(days=PAY_AHEAD_DAYS + 1))
            netdt = max(first_ok, d + timedelta(days=1))
            self._explicit_invoice(lifnr, "1000", min(netdt, d + timedelta(days=5)))
            self._bank_change(lifnr, ts, self._new_bank(), self._clerk(), None)
            self._add("R07", "unconfirmed_bank_change", lifnr, True, bukrs="1000", event_date=d)

        # Decoy: unconfirmed change on a vendor with nothing open.
        for lifnr in take(r.d07_unconfirmed_no_open_items):
            d = self.event_days[int(rng.integers(len(self.event_days)))]
            self.mode[lifnr] = "quiet"
            self._bank_change(lifnr, self._at(d, 11), self._new_bank(), self._clerk(), None)
            self._add("R07", "decoy_no_open_items", lifnr, False, event_date=d)

        # Background: legitimate bank changes, confirmed the same day; some future-dated.
        for lifnr in take(r.bank_change_legit):
            d = self.event_days[int(rng.integers(len(self.event_days)))]
            ts = self._at(d, int(rng.integers(8, 15)), int(rng.integers(60)))
            vf = None
            if rng.random() < r.bank_change_future_share:
                vf = datetime.combine(d + timedelta(days=int(rng.integers(2, 6))), time(0))
            approve = ts + timedelta(hours=1) if vf is None else vf + timedelta(hours=9)
            self._bank_change(lifnr, ts, self._new_bank(), self._clerk(), approve, valid_from=vf)
            self._add("BASE", "bank_change_future" if vf else "bank_change", lifnr, False,
                      event_date=d, detail=f"valid from {dats(vf.date())}" if vf else "")

        # Background: temporary payment blocks, set and lifted inside the window.
        for lifnr in take(r.temp_payment_block):
            i = int(rng.integers(len(self.event_days) - 2))
            on, off = self.event_days[i], self.event_days[min(i + 2, len(self.event_days) - 1)]
            self._field_change(lifnr, self._at(on, 10), "LFA1", (lifnr,), "SPERZ", "", "X")
            self._field_change(lifnr, self._at(off, 15), "LFA1", (lifnr,), "SPERZ", "X", "")
            self._add("BASE", "temp_payment_block", lifnr, False, event_date=on)

        # R06 alternative payee and one-time vendor exposure.
        for lifnr in take(r.r06_alt_payee_vendor):
            payee = pool.pop()
            self.lfa1[lifnr]["LNRZA"] = payee
            self._add("R06", "alt_payee_lfa1", lifnr, True, other=payee, detail="LFA1-LNRZA")
        for lifnr in take(r.r06_alt_payee_company_code):
            payee = pool.pop()
            self.lfb1[(lifnr, "1000")]["LNRZB"] = payee
            self._add("R06", "alt_payee_lfb1", lifnr, True, bukrs="1000", other=payee,
                      detail="LFB1-LNRZB")
        for lifnr in take(r.r06_payee_in_document):
            self.lfa1[lifnr]["XZEMP"] = "X"
            self._add("R06", "payee_in_document", lifnr, True, detail="LFA1-XZEMP")
        runs = self.event_runs
        for i, lifnr in enumerate(self.cpd_lifnrs):
            if i < self.cfg.one_time_repeat:
                pay_days = [runs[min(i + k, len(runs) - 1)] for k in (0, 2, 4)]
                self._add("R06", "one_time_repeat_payments", lifnr, True,
                          event_date=pay_days[1], detail="XCPDK, paid on 3 runs")
            else:
                pay_days = [runs[i % len(runs)]]
                self._add("R06", "decoy_one_time_single_payment", lifnr, False)
            for d in pay_days:  # ZTERM 0001: due on posting, paid by that day's run
                self._explicit_invoice(lifnr, "1000", d)

        self.changes.sort(key=lambda c: (c.ts, c.objectclas != "BUPA_BUP", c.objectid))
        for i, c in enumerate(self.changes, start=1):
            c.changenr = f"{500000 + i:010d}"
        # Vendors no rule or change touches; data quality defects are planted on these.
        self.untouched = [x for x in pool if self.mode[x] == "recent"]

    def plant_defects(self) -> None:
        """A few deliberate data quality defects so the silver expectations have work to do."""
        n = self.cfg.dq_defects_per_type
        pool = self.untouched
        adrc_of = {r["ADDRNUMBER"]: r for r in self.adrc}

        for _ in range(n):  # drop: bank_key_present
            lifnr = pool.pop()
            key = self.lfbk_key[lifnr]
            row = self.lfbk.pop(key)
            row["BANKN"] = ""
            self.lfbk_key[lifnr] = (*key[:3], "")
            self.lfbk[self.lfbk_key[lifnr]] = row
            self.but0bk[(self.partner_of[lifnr], "0001")]["BANKN"] = ""
            self._add("DQ", "bank_key_present", lifnr, True, detail="bank account number blank")
        for _ in range(n):  # warn: routing_number_9_digits
            lifnr = pool.pop()
            key = self.lfbk_key[lifnr]
            row = self.lfbk.pop(key)
            row["BANKL"] = key[2][:8]
            self.lfbk_key[lifnr] = (key[0], key[1], row["BANKL"], key[3])
            self.lfbk[self.lfbk_key[lifnr]] = row
            self.but0bk[(self.partner_of[lifnr], "0001")]["BANKL"] = row["BANKL"]
            self._add("DQ", "routing_number_9_digits", lifnr, True, detail="8-digit routing")
        for _ in range(n):  # warn: known_payment_terms
            lifnr = pool.pop()
            self.lfb1[(lifnr, "1000")]["ZTERM"] = "Z999"
            self._add("DQ", "known_payment_terms", lifnr, True, bukrs="1000",
                      detail="payment terms key not configured")
        missing_cvi = {pool.pop() for _ in range(n)}  # warn: has_business_partner
        self.cvi = [r for r in self.cvi if r["VENDOR"] not in missing_cvi]
        for lifnr in sorted(missing_cvi):
            self._add("DQ", "has_business_partner", lifnr, True, detail="CVI link missing")
        for _ in range(n):  # warn: us_address
            lifnr = pool.pop()
            adrc_of[self.lfa1[lifnr]["ADRNR"]]["COUNTRY"] = "CA"
            self._add("DQ", "us_address", lifnr, True, detail="address country CA")

        junk = []  # drop: known_doc_type
        for i in range(n):
            lifnr = pool.pop()
            d = self.event_days[i % len(self.event_days)]
            base = dict(RBUKRS="1000", BELNR=f"{1990000000 + i + 1:010d}", LIFNR=lifnr,
                        BLART="ZZ", _post=d, NETDT="", _clear=None, AUGBL="",
                        GJAHR=d.strftime("%Y"), BUDAT=dats(d), AUGDT_final="", RHCUR="USD")
            junk.append({**base, "DOCLN": "000001", "KOART": "K", "RACCT": RECON_ACCOUNT,
                         "HSL": "-500.00"})
            junk.append({**base, "DOCLN": "000002", "KOART": "S", "RACCT": EXPENSE_ACCOUNT,
                         "HSL": "500.00"})
            self._add("DQ", "known_doc_type", lifnr, True, bukrs="1000",
                      event_date=d, detail="document type ZZ")
        self.ledger = (pd.concat([self.ledger, pd.DataFrame(junk)], ignore_index=True)
                       .sort_values(["RBUKRS", "GJAHR", "BELNR", "DOCLN"])
                       .reset_index(drop=True))

    # ---------- journal ----------

    def _payable_fn(self):
        """Vendor can be paid at a run unless blocked or holding an unconfirmed change."""
        timeline: dict[str, list[tuple[datetime, str, str]]] = {}
        for c in self.changes:
            for it in c.items:
                if it.tab == "LFA1" and it.fname in ("CONFS", "SPERZ", "SPERR", "LOEVM"):
                    timeline.setdefault(c.objectid, []).append((c.ts, it.fname, it.new))

        def payable(lifnr: str, run: date) -> bool:
            base = self.lfa1[lifnr]
            state = {f: base[f] for f in ("CONFS", "SPERZ", "SPERR", "LOEVM")}
            at = datetime.combine(run, PAY_RUN_TIME)
            for ts, f, val in timeline.get(lifnr, []):
                if ts <= at:
                    state[f] = val
            return not any(state.values())

        return payable, set(timeline)

    def build_journal(self) -> None:
        rng, start = self.rng, self.cfg.start
        rows = []
        all_days = pd.bdate_range(start - timedelta(days=36 * 31), self.end).date
        hist_recent = [d for d in all_days if start - timedelta(days=365) <= d < start]
        hist_quiet = [d for d in all_days if start - timedelta(days=365) <= d
                      < start - timedelta(days=90)]
        hist_dormant = [d for d in all_days if start - timedelta(days=36 * 31) <= d
                        < start - timedelta(days=20 * 31)]
        scale = {}
        for lifnr, bukrs in sorted(self.lfb1):
            mode = self.mode[lifnr]
            if mode == "cpd":
                continue
            emp = lifnr in self.employee_lifnrs
            if lifnr not in scale:
                scale[lifnr] = 400.0 if emp else float(rng.lognormal(np.log(8000), 1.0))
            tc = 6 if emp else int(self.tx_count.get(lifnr, 1))
            primary = bukrs == "1000"
            if mode == "dormant":
                k, days = int(rng.integers(1, 4)), hist_dormant
            else:
                lam = min(tc, 24) / (2 if primary else 4)
                k = 1 + int(rng.poisson(lam))
                days = hist_quiet if mode == "quiet" else hist_recent
            for di in rng.integers(len(days), size=k):
                rows.append((lifnr, bukrs, days[di], scale[lifnr]))
            posting_blocked = self.lfa1[lifnr]["SPERR"] or self.lfa1[lifnr]["LOEVM"]
            if mode == "recent" and not posting_blocked:
                p = min(0.5, (k / 12) / 21)
                for d in self.weekdays:
                    if rng.random() < p:
                        rows.append((lifnr, bukrs, d, scale[lifnr]))

        inv = pd.DataFrame(rows, columns=["LIFNR", "BUKRS", "budat", "scale"])
        inv["amount"] = (inv["scale"] * rng.lognormal(0, 0.5, len(inv))).round(2)
        inv["term"] = [TERMS[self.lfb1[(a, b)]["ZTERM"]] for a, b in zip(inv.LIFNR, inv.BUKRS,
                                                                           strict=True)]
        inv["netdt"] = [b + timedelta(days=int(t)) for b, t in zip(inv.budat, inv.term,
                                                                  strict=True)]
        inv = pd.concat([inv[["LIFNR", "BUKRS", "budat", "netdt", "amount"]],
                         pd.DataFrame(self.explicit_invoices)], ignore_index=True)
        inv = inv.sort_values(["budat", "LIFNR", "BUKRS", "netdt", "amount"], kind="stable")
        inv = inv.reset_index(drop=True)

        # Pay each invoice at the first run on or after max(due - 3 days, posting date).
        runs = [d for d in all_days if d.weekday() in PAY_RUN_WEEKDAYS]
        payable, watched = self._payable_fn()
        run_of = []
        for lifnr, budat, netdt in zip(inv.LIFNR, inv.budat, inv.netdt, strict=True):
            i = bisect_left(runs, max(netdt - timedelta(days=PAY_AHEAD_DAYS), budat))
            while i < len(runs) and runs[i] >= start and (
                    lifnr in watched or self.lfa1[lifnr]["SPERZ"] or self.lfa1[lifnr]["SPERR"]
                    or self.lfa1[lifnr]["LOEVM"]) and not payable(lifnr, runs[i]):
                i += 1
            run_of.append(runs[i] if i < len(runs) else None)
        inv["run"] = run_of

        inv["BELNR"] = [f"{1900000000 + i:010d}" for i in range(1, len(inv) + 1)]
        paid = inv[inv["run"].notna()]
        pay = (paid.groupby(["run", "LIFNR", "BUKRS"], as_index=False)["amount"].sum()
               .sort_values(["run", "LIFNR", "BUKRS"]).reset_index(drop=True))
        pay["BELNR"] = [f"{1500000000 + i:010d}" for i in range(1, len(pay) + 1)]
        belnr_of_pay = {(r.run, r.LIFNR, r.BUKRS): r.BELNR for r in pay.itertuples()}
        inv["AUGBL"] = [belnr_of_pay.get((r, a, b), "") if isinstance(r, date) else ""
                        for r, a, b in zip(inv.run, inv.LIFNR, inv.BUKRS, strict=True)]

        # Offset lines also carry LIFNR here, so the KOART = 'K' filter matters.
        def lines(df, blart, k_sign, s_account, cleared_col, netdt_col):
            k = pd.DataFrame({
                "RBUKRS": df.BUKRS, "BELNR": df.BELNR, "DOCLN": "000001", "KOART": "K",
                "RACCT": RECON_ACCOUNT, "LIFNR": df.LIFNR, "BLART": blart, "_post": df._post,
                "HSL": (k_sign * df.amount).round(2), "NETDT": df[netdt_col],
                "_clear": df[cleared_col], "AUGBL": df.AUGBL})
            s = k.copy()
            s["DOCLN"], s["KOART"], s["RACCT"] = "000002", "S", s_account
            s["HSL"], s["NETDT"], s["_clear"], s["AUGBL"] = -k["HSL"], None, None, ""
            return pd.concat([k, s])

        inv["_post"] = inv["budat"]
        pay["_post"] = pay["netdt"] = pay["clear"] = pay["run"]
        pay["AUGBL"] = pay["BELNR"]
        inv["clear"] = inv["run"]
        led = pd.concat([lines(inv, "KR", -1, EXPENSE_ACCOUNT, "clear", "netdt"),
                         lines(pay, "KZ", 1, BANK_CLEARING_ACCOUNT, "clear", "netdt")])
        led["GJAHR"] = [d.strftime("%Y") for d in led._post]
        led["BUDAT"] = [dats(d) for d in led._post]
        led["NETDT"] = [dats(d) if isinstance(d, date) else "" for d in led.NETDT]
        led["AUGDT_final"] = [dats(d) if isinstance(d, date) else "" for d in led._clear]
        led["HSL"] = [f"{x:.2f}" for x in led.HSL]
        led["RHCUR"] = "USD"
        self.ledger = led.sort_values(["RBUKRS", "GJAHR", "BELNR", "DOCLN"]).reset_index(drop=True)
        self.invoices, self.payments = inv, pay

    # ---------- extracts ----------

    def _apply(self, c: Change) -> None:
        for it in c.items:
            if it.tab == "LFA1":
                self.lfa1[it.key[0]][it.fname] = it.new
            elif it.tab == "LFB1":
                self.lfb1[it.key][it.fname] = it.new
            elif it.tab == "LFBK":
                if it.chngind == "D":
                    self.lfbk.pop(it.key, None)
                else:
                    self.lfbk[it.key] = dict(it.payload)
            elif it.tab == "BUT0BK":
                if it.chngind == "I":
                    self.but0bk[it.key] = dict(it.payload)
                else:
                    self.but0bk[it.key][it.fname] = it.new

    def _change_rows(self, changes: list[Change]) -> tuple[pd.DataFrame, pd.DataFrame]:
        hdr, pos = [], []
        for c in changes:
            hdr.append(dict(OBJECTCLAS=c.objectclas, OBJECTID=c.objectid, CHANGENR=c.changenr,
                            USERNAME=c.user, UDATE=dats(c.ts.date()), UTIME=tims(c.ts),
                            TCODE=c.tcode, CHANGE_IND="U"))
            for it in c.items:
                pos.append(dict(OBJECTCLAS=c.objectclas, OBJECTID=c.objectid,
                                CHANGENR=c.changenr, TABNAME=it.tab,
                                TABKEY=tabkey(it.tab, it.key), FNAME=it.fname,
                                CHNGIND=it.chngind, VALUE_NEW=it.new, VALUE_OLD=it.old))
        return (pd.DataFrame(hdr, columns=CDHDR_COLS), pd.DataFrame(pos, columns=CDPOS_COLS))

    def emit(self) -> dict[date, dict[str, pd.DataFrame]]:
        out = {}
        led = self.ledger
        static = {
            "ADRC": pd.DataFrame(self.adrc).sort_values("ADDRNUMBER"),
            "BUT000": pd.DataFrame(self.but000).sort_values("PARTNER"),
            "DFKKBPTAXNUM": pd.DataFrame(self.taxnum).sort_values("PARTNER"),
            "CVI_VEND_LINK": pd.DataFrame(self.cvi).sort_values("VENDOR"),
        }
        pending = list(self.changes)
        for d in self.days:
            todays = [c for c in pending if c.ts.date() == d]
            for c in todays:
                self._apply(c)
            tables = {k: v.reset_index(drop=True) for k, v in static.items()}
            tables["LFA1"] = pd.DataFrame(self.lfa1.values(), columns=LFA1_COLS)
            tables["LFB1"] = pd.DataFrame(self.lfb1.values(), columns=LFB1_COLS)
            tables["LFBK"] = pd.DataFrame(self.lfbk.values(), columns=LFBK_COLS)
            tables["BUT0BK"] = pd.DataFrame(self.but0bk.values(), columns=BUT0BK_COLS)
            for t, keys in [("LFA1", ["LIFNR"]), ("LFB1", ["LIFNR", "BUKRS"]),
                            ("LFBK", ["LIFNR", "BANKL", "BANKN"]),
                            ("BUT0BK", ["PARTNER", "BKVID"])]:
                tables[t] = tables[t].sort_values(keys).reset_index(drop=True)

            if d == self.cfg.start:
                m = led._post <= d
            else:
                m = (led._post == d) | (led._clear.eq(d) & (led._post < d))
            day = led[m].copy()
            cleared = day._clear.map(lambda x, d=d: isinstance(x, date) and x <= d)
            day["AUGBL"] = day["AUGBL"].where(cleared, "")
            day["AUGDT"] = day["AUGDT_final"].where(cleared, "")
            day = pd.concat([day.assign(RLDNR="0L"), day.assign(RLDNR="2L")])
            tables["ACDOCA"] = (day[ACDOCA_COLS]
                                .sort_values(["RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN"])
                                .reset_index(drop=True))
            tables["CDHDR"], tables["CDPOS"] = self._change_rows(todays)
            out[d] = {t: tables[t].astype(str) for t in MASTER_TABLES + DELTA_TABLES}
        return out

    def run(self) -> Output:
        self.build_master()
        self.plan()
        self.build_journal()
        self.plant_defects()
        extracts = self.emit()
        counts = pd.DataFrame({dats(d): {t: len(df) for t, df in tabs.items()}
                               for d, tabs in extracts.items()})
        manifest = pd.DataFrame(self.manifest).sort_values(
            ["rule_id", "variant", "LIFNR", "BUKRS"]).reset_index(drop=True)
        return Output(extracts, manifest, self.key_map.reset_index(drop=True), counts)
