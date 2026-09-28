import re
from pathlib import Path

from generator.config import Config


def test_initial_load_date_matches_generator_start():
    bundle = Path("databricks.yml").read_text(encoding="utf-8")
    block = bundle.split("initial_load_date:", 1)[1]
    default = re.search(r'default:\s*"(\d{4}-\d{2}-\d{2})"', block).group(1)
    assert default == Config().start.isoformat()
