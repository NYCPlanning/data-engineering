"""
Pull raw HTML pages from a live EDDE site deploy for manual QA.

Fetches every (geography x subgroup) combination for one category and saves
the raw HTML to output/raw/, one file per URL, for parse_site_pages.py and
manual review. See CHECKLIST.md for the list of URLs this produces.

Usage:
    python pull_site_pages.py
"""

from pathlib import Path

import requests

# --- Configuration: edit these to point at a different deploy / geography ---

BASE_URL = "https://74be08--equity-tool.netlify.app"

# District: Park Slope & Carroll Gardens
PUMA = "4306"
# Borough: Brooklyn
BOROUGH = "3"

CATEGORY = "demo"
SUBGROUPS = ["tot", "anh", "bnh", "hsp", "wnh"]

GEOGRAPHIES = [
    ("district", PUMA),
    ("borough", BOROUGH),
    ("citywide", "nyc"),
]

# --- End configuration ---

OUTPUT_DIR = Path(__file__).parent / "output" / "raw"


def build_targets() -> list[tuple[str, str]]:
    """Returns (output_filename_stem, url) for every geography x subgroup pair."""
    targets = []
    for geo_level, geo_id in GEOGRAPHIES:
        for subgroup in SUBGROUPS:
            filename = f"{geo_level}_{geo_id}_{CATEGORY}_{subgroup}"
            url = f"{BASE_URL}/data/{geo_level}/{geo_id}/{CATEGORY}/{subgroup}"
            targets.append((filename, url))
    return targets


def pull():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    targets = build_targets()
    print(f"Pulling {len(targets)} pages to {OUTPUT_DIR}")

    failures = []
    for filename, url in targets:
        out_path = OUTPUT_DIR / f"{filename}.html"
        try:
            resp = requests.get(url, timeout=30)
        except requests.RequestException as e:
            print(f"[ERROR] {url} -> {e}")
            failures.append(url)
            continue

        out_path.write_text(resp.text)
        flag = "OK" if resp.status_code == 200 else "FAIL"
        if flag == "FAIL":
            failures.append(url)
        print(
            f"[{flag}] {resp.status_code} {url} -> {out_path.name} "
            f"({len(resp.text)} bytes)"
        )

    if failures:
        print(f"\n{len(failures)} failed:")
        for url in failures:
            print(f"  {url}")
    else:
        print("\nAll pages pulled successfully.")


if __name__ == "__main__":
    pull()
