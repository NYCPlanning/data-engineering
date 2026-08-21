"""Apply `db-*` product labels, topic labels, and issue types by matching issue titles.

Both were applied by hand for years and landed on a minority of the issues they
should have — product labels on 18% of the issues naming a product, the Bug type
on 61% of scheduled-action failures — so filtering the board by either never
worked. Titles carry both signals reliably enough to drive them instead.

    python admin/ops/issue_metadata.py match "PLUTO - migrate build SQL"
    python admin/ops/issue_metadata.py apply --issue 2549 --write
    python admin/ops/issue_metadata.py backfill            # dry run
    python admin/ops/issue_metadata.py backfill --write
"""

import argparse
import json
import re
import subprocess
from functools import lru_cache
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[2] / ".github" / "issue_metadata.yml"
REPO = "NYCPlanning/data-engineering"
LIST_FIELDS = "number,title,labels,issueType"


Rules = list[tuple[str, re.Pattern]]
TopicRules = list[tuple[str, re.Pattern, bool]]


@lru_cache(maxsize=1)
def _rules() -> tuple[Rules, TopicRules, Rules]:
    """(label, pattern) for products and types; topics also carry their exclusivity."""
    config = yaml.safe_load(CONFIG_PATH.read_text())

    def compile_specs(specs, key):
        return [
            (spec[key] if key else name, re.compile("|".join(spec["patterns"]), re.I))
            for name, spec in specs.items()
        ]

    topics = [
        (name, pattern, config["labels"][name].get("exclusive", False))
        for name, pattern in compile_specs(config.get("labels", {}), None)
    ]
    return (
        compile_specs(config["products"], "label"),
        topics,
        compile_specs(config.get("types", {}), None),
    )


def match_labels(title: str) -> list[str]:
    """Product labels, plus every topic label that matches.

    An `exclusive` topic is dropped when a product matched too: `platform` means
    work that isn't a data product, so `PLUTO docker image` stays PLUTO's. Stage
    labels like `ingest/library` describe *where* in the pipeline a product
    broke, so they stack with the product label.
    """
    products, topics, _ = _rules()
    from_product = [label for label, pattern in products if pattern.search(title)]
    from_topic = [
        label
        for label, pattern, exclusive in topics
        if pattern.search(title) and not (exclusive and from_product)
    ]
    return sorted(from_product + from_topic)


def match_type(title: str) -> str | None:
    """First matching issue type, or None. Types are mutually exclusive."""
    return next((name for name, p in _rules()[2] if p.search(title)), None)


def _gh(*args: str) -> str:
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True
    ).stdout


def _changes(issue: dict) -> tuple[list[str], str | None]:
    """Labels to add and the type to set, given what the issue already carries."""
    existing = {label["name"] for label in issue["labels"]}
    labels = [m for m in match_labels(issue["title"]) if m not in existing]
    # Never overwrite a type someone chose deliberately.
    issue_type = None if issue.get("issueType") else match_type(issue["title"])
    return labels, issue_type


def _write(number: int, labels: list[str], issue_type: str | None) -> None:
    args = [arg for label in labels for arg in ("--add-label", label)]
    if issue_type:
        args += ["--type", issue_type]
    _gh("issue", "edit", str(number), "--repo", REPO, *args)


def _describe(labels: list[str], issue_type: str | None) -> str:
    return " ".join(
        [
            "+" + ",".join(labels) if labels else "",
            f"[{issue_type}]" if issue_type else "",
        ]
    ).strip()


def apply_one(number: int, write: bool) -> str:
    issue = json.loads(
        _gh("issue", "view", str(number), "--repo", REPO, "--json", LIST_FIELDS)
    )
    labels, issue_type = _changes(issue)
    if (labels or issue_type) and write:
        _write(number, labels, issue_type)
    return _describe(labels, issue_type)


def backfill(write: bool) -> None:
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
            LIST_FIELDS,
        )
    )
    changed = 0
    for issue in issues:
        labels, issue_type = _changes(issue)
        if not labels and not issue_type:
            continue
        changed += 1
        print(
            f"#{issue['number']:<6} {_describe(labels, issue_type):<34} {issue['title'][:52]}"
        )
        if write:
            _write(issue["number"], labels, issue_type)
    verb = "updated" if write else "would update"
    print(f"\n{verb} {changed} of {len(issues)} issues")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_match = sub.add_parser("match", help="print metadata a title would get")
    p_match.add_argument("title")

    p_apply = sub.add_parser("apply", help="update a single issue")
    p_apply.add_argument("--issue", type=int, required=True)
    p_apply.add_argument("--write", action="store_true")

    p_backfill = sub.add_parser("backfill", help="update every matching issue")
    p_backfill.add_argument("--write", action="store_true")

    args = parser.parse_args()
    if args.command == "match":
        print(
            _describe(match_labels(args.title), match_type(args.title)) or "(no match)"
        )
    elif args.command == "apply":
        print(apply_one(args.issue, args.write) or "nothing to change")
    else:
        backfill(args.write)


if __name__ == "__main__":
    main()
