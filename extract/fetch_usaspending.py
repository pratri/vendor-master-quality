"""Download USAspending Award Data Archive files (full fiscal year, per agency).

Usage:
    python -m extract.fetch_usaspending --agency 014 --years 2025 2026

Files land in data/raw/ (gitignored). Already-downloaded files are skipped.
"""

import argparse
import time
from pathlib import Path

import requests

BASE = "https://files.usaspending.gov/award_data_archive"
# Archive files are regenerated monthly and the date stamp is part of the name.
DEFAULT_STAMP = "20260906"
RAW_DIR = Path("data/raw")
KINDS = ("Contracts", "Assistance")


def archive_url(agency: str, fy: int, kind: str, stamp: str) -> str:
    return f"{BASE}/FY{fy}_{agency}_{kind}_Full_{stamp}.zip"


def download(url: str, dest: Path, attempts: int = 20) -> None:
    tmp = dest.with_suffix(".part")
    for i in range(1, attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                done = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        done += len(chunk)
            if total and done != total:
                raise OSError(f"short read: {done} of {total} bytes")
            tmp.replace(dest)
            print(f"  ok {dest.name} ({done / 1e6:.1f} MB)", flush=True)
            return
        except (requests.RequestException, OSError) as e:
            # The server blocks bursts of requests for a while, so back off up to 5 minutes.
            wait = min(60 * i, 300)
            print(f"  attempt {i} failed: {e}. Retrying in {wait}s", flush=True)
            time.sleep(wait)
    raise SystemExit(f"Giving up on {url}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--agency", default="014", help="toptier agency code, e.g. 014 = Interior")
    p.add_argument("--years", nargs="+", type=int, default=[2025, 2026])
    p.add_argument("--stamp", default=DEFAULT_STAMP)
    args = p.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for fy in args.years:
        for kind in KINDS:
            url = archive_url(args.agency, fy, kind, args.stamp)
            dest = RAW_DIR / url.rsplit("/", 1)[1]
            if dest.exists():
                print(f"  skip {dest.name} (already downloaded)", flush=True)
                continue
            print(f"Downloading {url}", flush=True)
            download(url, dest)
            time.sleep(5)  # be polite, the server rate-limits


if __name__ == "__main__":
    main()
