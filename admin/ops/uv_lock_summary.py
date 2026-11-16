"""Summarize `uv lock --upgrade` output as markdown, for the weekly upgrade PR body.

The uv.lock diff is mostly hash and wheel-URL churn, so it hides what actually moved.
uv already prints one line per changed package; this groups them by size of bump and
flags the two things worth stopping for: downgrades, and packages that now resolve to
more than one version (both symptoms of the resolver forking, e.g. old dbt on 3.11).

    uv lock --upgrade 2>&1 | tee uv_lock.log
    python admin/ops/uv_lock_summary.py uv_lock.log > summary.md

Stdlib only: the workflow runs it before any environment is synced.
"""

import re
import sys
from pathlib import Path

LINE = re.compile(
    r"^(?P<action>Updated|Added|Removed) (?P<name>\S+) (?P<old>v[^>]*?)(?: -> (?P<new>v.*))?$"
)


def _versions(field: str | None) -> list[str]:
    return [v.strip().removeprefix("v") for v in (field or "").split(",") if v.strip()]


def _key(version: str) -> tuple[int, ...]:
    return tuple(int(n) for n in re.findall(r"\d+", version))


def _bump(old: str, new: str) -> str:
    o, n = _key(old), _key(new)
    if n < o:
        return "downgrade"
    if o[:1] != n[:1]:
        return "major"
    if o[1:2] != n[1:2]:
        return "minor"
    return "patch"


def parse(log: str) -> dict[str, list[tuple[str, str, str]]]:
    groups: dict[str, list[tuple[str, str, str]]] = {
        k: []
        for k in ("downgrade", "multi", "major", "minor", "patch", "added", "removed")
    }
    for line in log.splitlines():
        m = LINE.match(line.strip())
        if not m:
            continue
        name, action = m["name"], m["action"]
        if action == "Added":
            groups["added"].append((name, "", ", ".join(_versions(m["old"]))))
            continue
        if action == "Removed":
            groups["removed"].append((name, ", ".join(_versions(m["old"])), ""))
            continue
        old, new = _versions(m["old"]), _versions(m["new"])
        row = (name, ", ".join(old), ", ".join(new))
        if len(new) > 1:
            groups["multi"].append(row)
        else:
            groups[_bump(max(old, key=_key), new[0])].append(row)
    return groups


# (group, heading, label in the header counts, collapsed)
SECTIONS = [
    ("downgrade", "⚠️ Downgrades", "downgraded", False),
    ("multi", "⚠️ Split across versions", "split", False),
    ("major", "Major", "major", False),
    ("minor", "Minor", "minor", False),
    ("added", "Added", "added", False),
    ("removed", "Removed", "removed", False),
    ("patch", "Patch", "patch", True),
]


def render(groups: dict[str, list[tuple[str, str, str]]]) -> str:
    if not any(groups.values()):
        return "## uv.lock changes\n\nNo package versions changed.\n"
    total = sum(
        len(groups[k]) for k in ("downgrade", "multi", "major", "minor", "patch")
    )
    counts = ", ".join(
        f"{len(groups[k])} {label}" for k, _, label, _ in SECTIONS if groups[k]
    )
    out = ["## uv.lock changes", "", f"{total} packages updated ({counts})."]
    if groups["downgrade"] or groups["multi"]:
        out += [
            "",
            "> [!WARNING]",
            "> The resolver moved packages backwards or split them across versions. That usually",
            "> means a fork (e.g. by Python version) picked an old release; add a floor on `main`.",
        ]
    for key, title, _, collapsed in SECTIONS:
        rows = groups[key]
        if not rows:
            continue
        table = ["| Package | From | To |", "|---|---|---|"] + [
            f"| `{n}` | {o} | {v} |" for n, o, v in sorted(rows)
        ]
        if collapsed:
            out += ["", f"<details><summary>{title} ({len(rows)})</summary>", ""]
            out += table + ["", "</details>"]
        else:
            out += ["", f"### {title} ({len(rows)})", ""] + table
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    sys.stdout.write(render(parse(Path(sys.argv[1]).read_text())))
