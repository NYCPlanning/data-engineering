"""
Parse the raw HTML pulled by pull_site_pages.py into plain-text digests for
manual QA smell-testing.

Each page renders one indicator title (<h3>) followed by several adjacent
<table> elements - one per vintage/change column set (e.g. earliest, current,
change). This walks the DOM in document order, tracking the most recent <h3>,
and dumps every table's header + data rows under it.

Usage:
    python parse_site_pages.py
"""

from pathlib import Path

from bs4 import BeautifulSoup
from bs4.element import Tag

RAW_DIR = Path(__file__).parent / "output" / "raw"
PARSED_DIR = Path(__file__).parent / "output" / "parsed"


def clean_soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, "html.parser")
    # Chakra/emotion injects <style> tags as siblings inside cells; svg icons
    # also show up as button/link decoration. Both pollute get_text().
    for tag in soup.find_all(["style", "svg"]):
        tag.decompose()
    return soup


def table_to_lines(table: Tag) -> list[str]:
    lines = []
    for section_name in ("thead", "tbody"):
        section = table.find(section_name)
        if not section:
            continue
        for tr in section.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if any(cells):
                lines.append("  | " + " | ".join(cells))
    return lines


def parse_page(html: str) -> str:
    soup = clean_soup(html)
    out_lines = []

    title = soup.title.get_text(strip=True) if soup.title else ""
    out_lines.append(f"PAGE TITLE: {title}")

    # Headings before the first table are page-level breadcrumbs (site name,
    # PUMA/borough code, geography name, category name).
    first_table = soup.find("table")
    if first_table:
        breadcrumbs = list(reversed(first_table.find_all_previous(["h1", "h2", "h3"])))
    else:
        breadcrumbs = soup.find_all(["h1", "h2", "h3"])
    for h in breadcrumbs:
        out_lines.append(f"{h.name.upper()}: {h.get_text(strip=True)}")
    out_lines.append("")

    if not first_table:
        out_lines.append("!!! NO TABLES FOUND ON THIS PAGE !!!")
        return "\n".join(out_lines)

    current_h3 = None
    for el in soup.find_all(["h3", "table"]):
        if el.name == "h3":
            current_h3 = el.get_text(strip=True)
            continue
        out_lines.append(f"### INDICATOR: {current_h3}")
        rows = table_to_lines(el)
        if not rows:
            out_lines.append("  (empty table)")
        out_lines.extend(rows)
        out_lines.append("")

    return "\n".join(out_lines)


def parse_all():
    PARSED_DIR.mkdir(parents=True, exist_ok=True)
    html_files = sorted(RAW_DIR.glob("*.html"))
    if not html_files:
        print(f"No HTML files found in {RAW_DIR} - run pull_site_pages.py first.")
        return

    print(f"Parsing {len(html_files)} files from {RAW_DIR}")
    for f in html_files:
        text = parse_page(f.read_text())
        out_path = PARSED_DIR / f"{f.stem}.txt"
        out_path.write_text(text)
        print(f"  {f.name} -> {out_path.name}")


if __name__ == "__main__":
    parse_all()
