from pathlib import Path

import duckdb
from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"


def test_demo_runs_and_opens_on_latest_date():
    at = AppTest.from_file(str(APP), default_timeout=120).run()
    assert not at.exception
    latest = duckdb.sql("SELECT max(extract_date) FROM read_parquet('app/data/vendor_risk.parquet')"
                        ).fetchone()[0]
    assert str(at.selectbox[0].value)[:10] == str(latest)[:10]
    labels = [m.label for m in at.metric]
    assert "Vendors with exceptions" in labels
    assert any(label.startswith("R02") for label in labels)
