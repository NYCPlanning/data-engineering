"""Apply `db-*` product labels to issues by matching their titles.

Product labels were applied by hand for years and landed on 18% of the issues
that named a product, so filtering the board by product never worked. Titles
name the product reliably enough to drive the labels instead.

    python admin/ops/product_labels.py match "PLUTO - migrate build SQL"
    python admin/ops/product_labels.py label --issue 2549 --apply
    python admin/ops/product_labels.py backfill            # dry run
    python admin/ops/product_labels.py backfill --apply
"""

import argparse
import json
import re
import subprocess
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[2] / ".github" / "product_labels.yml"
REPO = "NYCPlanning/data-engineering"


@lru_cache(maxsize=1)
def _rules() -> list[tuple[str, re.Pattern]]:
    """Label name paired with one compiled pattern alternating over its aliases."""
    config = yaml.safe_load(CONFIG_PATH.read_text())
    return [
        (spec["label"], re.compile("|".join(spec["patterns"]), re.IGNORECASE))
        for spec in config["products"].values()
    ]


def match_labels(title: str) -> list[str]:
    return sorted(label for label, pattern in _rules() if pattern.search(title))


def _gh(*args: str) -> str:
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True
    ).stdout


def _apply(number: int, missing: list[str]) -> None:
    _gh(
        "issue",
        "edit",
        str(number),
        "--repo",
        REPO,
        *sum((["--add-label", m] for m in missing), []),
    )


def label_one(number: int, apply: bool) -> list[str]:
    issue = json.loads(
        _gh("issue", "view", str(number), "--repo", REPO, "--json", "title,labels")
    )
    existing = {label["name"] for label in issue["labels"]}
    missing = [m for m in match_labels(issue["title"]) if m not in existing]
    if missing and apply:
        _apply(number, missing)
    return missing


def backfill(apply: bool) -> None:
    issues = json.loads(
        _gh(
            "issue",
            "list",
            "--repo",
            REPO,
            "--state",
            "all",
            "--limit",
            "5000",
            "--json",
            "number,title,labels",
        )
    )
    changed = 0
    for issue in issues:
        existing = {label["name"] for label in issue["labels"]}
        missing = [m for m in match_labels(issue["title"]) if m not in existing]
        if not missing:
            continue
        changed += 1
        print(f"#{issue['number']:<6} +{','.join(missing):<24} {issue['title'][:60]}")
        if apply:
            _apply(issue["number"], missing)
    verb = "labeled" if apply else "would label"
    print(f"\n{verb} {changed} of {len(issues)} issues")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_match = sub.add_parser("match", help="print labels a title would get")
    p_match.add_argument("title")

    p_label = sub.add_parser("label", help="label a single issue")
    p_label.add_argument("--issue", type=int, required=True)
    p_label.add_argument("--apply", action="store_true")

    p_backfill = sub.add_parser("backfill", help="label every matching issue")
    p_backfill.add_argument("--apply", action="store_true")

    args = parser.parse_args()
    if args.command == "match":
        print("\n".join(match_labels(args.title)) or "(no product matched)")
    elif args.command == "label":
        added = label_one(args.issue, args.apply)
        verb = "added" if args.apply else "would add"
        print(f"{verb}: {','.join(added)}" if added else "no labels to add")
    else:
        backfill(args.apply)


if __name__ == "__main__":
    main()
