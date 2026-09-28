"""Location of the landing volume, read from the deployed bundle."""

import json
import subprocess


def landing_volume() -> str:
    """dbfs:/Volumes/... path of the landing volume (dev mode prefixes the schema per user)."""
    summary = subprocess.run(["databricks", "bundle", "summary", "--output", "json"],
                             check=True, capture_output=True, text=True).stdout
    full_name = json.loads(summary)["resources"]["volumes"]["landing"]["id"]
    return "dbfs:/Volumes/" + full_name.replace(".", "/")
