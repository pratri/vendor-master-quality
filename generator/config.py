"""Generator settings. Everything is seeded, so the same config gives the same extracts."""

from dataclasses import dataclass, field
from datetime import date


@dataclass(frozen=True)
class Rates:
    """Injection rates as a share of vendors (R05 is per vendor-company code)."""

    r02_change_pay_revert: float = 0.002
    r02_same_day_share: float = 0.5
    r03_shared_vendor_pairs: float = 0.001
    r03_employee_shares: float = 0.0005
    r04_dormant_unblocked: float = 0.008
    r04_dormant_sperm_only: float = 0.002
    r05_reprf_blank: float = 0.01
    r05_changed_in_window_share: float = 0.2
    r07_unconfirmed_open_items: float = 0.002
    # Decoys: look similar but must not be flagged
    d02_change_revert_no_payment: float = 0.001
    d07_unconfirmed_no_open_items: float = 0.001
    # Background activity
    blocked_natural: float = 0.03
    bank_change_legit: float = 0.005
    bank_change_future_share: float = 0.5
    temp_payment_block: float = 0.003


@dataclass(frozen=True)
class Config:
    seed: int = 42
    start: date = date(2026, 9, 1)
    days: int = 10
    employees: int = 150
    second_cc_share: float = 0.5
    third_cc_share: float = 0.2
    rates: Rates = field(default_factory=Rates)
