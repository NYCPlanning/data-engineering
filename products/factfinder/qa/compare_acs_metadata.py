"""
Compare ACS metadata JSON files between two years to identify new and dropped variables.

Usage:
    python compare_acs_metadata.py [current_year] [previous_year]

Defaults to comparing the two most recent years found under factfinder/data/acs/.
"""

import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "factfinder" / "data" / "acs"


def load(year: str) -> list[dict]:
    path = DATA_DIR / year / "metadata.json"
    with open(path) as f:
        return json.load(f)


def available_years() -> list[str]:
    return sorted(p.name for p in DATA_DIR.iterdir() if p.is_dir())


def compare(
    current: list[dict],
    previous: list[dict],
    out_path: Path,
    current_year: str,
    previous_year: str,
) -> None:
    current_vars = {r["pff_variable"]: r for r in current}
    previous_vars = {r["pff_variable"]: r for r in previous}

    new = {v: r for v, r in current_vars.items() if v not in previous_vars}
    dropped = {v: r for v, r in previous_vars.items() if v not in current_vars}

    def fmt(records: dict) -> str:
        rows = sorted(
            records.values(),
            key=lambda r: (r["domain"], r["category"], r["pff_variable"]),
        )
        col_w = [
            max(len(r[k]) for r in rows) for k in ("pff_variable", "category", "domain")
        ]
        header = f"  {'pff_variable':<{col_w[0]}}  {'category':<{col_w[1]}}  {'domain':<{col_w[2]}}"
        sep = "  " + "  ".join("-" * w for w in col_w)
        lines = [header, sep]
        for r in rows:
            lines.append(
                f"  {r['pff_variable']:<{col_w[0]}}  {r['category']:<{col_w[1]}}  {r['domain']:<{col_w[2]}}"
            )
        return "\n".join(lines)

    lines = []
    lines.append(f"Current year:  {current_year}")
    lines.append(f"Previous year: {previous_year}")
    lines.append("")
    lines.append(
        f"New variables in current ({len(new)}) — probably expected, confirm intentional additions:"
    )
    lines.append(fmt(new) if new else "  (none)")
    lines.append("")
    lines.append(
        f"Dropped variables from previous ({len(dropped)}) — likely a problem if unexpected:"
    )
    lines.append(fmt(dropped) if dropped else "  (none)")

    output = "\n".join(lines)
    print(output)
    out_path.write_text(output + "\n")
    print(f"\nResults saved to {out_path}")


def main() -> None:
    years = available_years()
    if len(years) < 2:
        sys.exit(f"Need at least two year folders under {DATA_DIR}, found: {years}")

    if len(sys.argv) == 3:
        current_year, previous_year = sys.argv[1], sys.argv[2]
    else:
        sys.exit("Usage: compare_acs_metadata.py [current_year previous_year]")

    if current_year <= previous_year:
        sys.exit(f"Error: current_year ({current_year}) must be greater than previous_year ({previous_year})")
    out_path = DATA_DIR / current_year / "metadata_diffs.txt"
    compare(
        load(current_year), load(previous_year), out_path, current_year, previous_year
    )


if __name__ == "__main__":
    main()
