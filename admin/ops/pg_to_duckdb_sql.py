"""Scan (and optionally fix) SQL and dbt yml for the postgres/PostGIS -> DuckDB spelling
differences found while porting green_fast_track's dbt build to DuckDB (see
products/green_fast_track/issues.md). Most of these aren't safe to blindly rewrite -
argument shapes, semantics, or context (aggregate vs. scalar, e.g.) differ enough that a
human has to look. Only the purely cosmetic renames (geometry-type string literals,
`geometrytype()` -> `ST_GeometryType()`, typed/SRID-modified geometry columns like
`geometry(Point, 2263)` -> plain `geometry`) are auto-fixed; everything else is reported
as a flag for review.

    python admin/ops/pg_to_duckdb_sql.py products/some_product          # dry run, report only
    python admin/ops/pg_to_duckdb_sql.py products/some_product --write  # also apply safe fixes
    python admin/ops/pg_to_duckdb_sql.py products/some_product --ext .sql,.yml,.yaml
"""

import argparse
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Rule:
    name: str
    pattern: re.Pattern
    replacement: str | None  # None => flag-only, no safe automatic fix
    note: str


RULES: list[Rule] = [
    # PostGIS's ST_GeometryType()/geometrytype() return 'ST_MultiPoint'-style names;
    # DuckDB's ST_GeometryType() returns plain 'MULTIPOINT'-style names. Comparisons
    # against a quoted literal are a pure string swap - the two sides always mean
    # the same geometry family, regardless of context.
    *(
        Rule(
            f"geom-type-literal-{pg}",
            re.compile(rf"'{pg}'"),
            f"'{duck}'",
            "PostGIS-style geometry type name in a quoted comparison -> DuckDB's plain name.",
        )
        for pg, duck in [
            ("ST_MultiPoint", "MULTIPOINT"),
            ("ST_MultiPolygon", "MULTIPOLYGON"),
            ("ST_MultiLineString", "MULTILINESTRING"),
            ("ST_GeometryCollection", "GEOMETRYCOLLECTION"),
            ("ST_Point", "POINT"),
            ("ST_Polygon", "POLYGON"),
            ("ST_LineString", "LINESTRING"),
        ]
    ),
    Rule(
        "geometrytype-function",
        re.compile(r"\bgeometrytype\s*\(", re.I),
        "ST_GeometryType(",
        "PostGIS's lowercase alias -> DuckDB's ST_GeometryType(); same signature and return shape.",
    ),
    # PostGIS typed geometry columns carry a subtype and SRID modifier - geometry(Point,
    # 4326), geometry(MultiPolygon, 2263), the bare geometry(Geometry, srid), etc. DuckDB's
    # spatial GEOMETRY type takes no modifier at all (no subtype, no SRID), so the whole
    # parenthesized part is just dropped. Shows up in dbt yml (seed column_types, model
    # contract data_type) and in raw SQL (CREATE TABLE column types, ::geometry(...) casts) -
    # the fix is identical either way, so this one rule covers both.
    Rule(
        "typed-geometry-column",
        re.compile(r"\bgeometry\s*\([^)]*\)", re.I),
        "geometry",
        "PostGIS typed/SRID-modified geometry column -> DuckDB's unmodified geometry type.",
    ),
    # Below: real, recurring differences worth flagging every time they show up, but each
    # needs a human to look at the surrounding query - a blind regex swap would either
    # change behavior or just not compile in DuckDB.
    Rule(
        "st_union-aggregate",
        re.compile(r"\bST_Union\s*\(", re.I),
        None,
        "DuckDB has no aggregate ST_Union - if this is used as an aggregate (one geometry "
        "arg, grouped), rewrite to ST_Union_Agg(). DuckDB's 2-arg scalar ST_Union(a, b) is "
        "unaffected and doesn't need to change.",
    ),
    Rule(
        "st_relate",
        re.compile(r"\bST_Relate\s*\(", re.I),
        None,
        "No DuckDB equivalent for arbitrary DE-9IM patterns. Common cases can often be "
        "rewritten as a combination of ST_Intersects/ST_Touches/ST_Contains etc.",
    ),
    Rule(
        "st_setsrid",
        re.compile(r"\bST_SetSRID\s*\(", re.I),
        None,
        "DuckDB's equivalent is ST_SetCRS(geom, 'EPSG:xxxx') - a string CRS, not the "
        "numeric SRID postgres takes. Argument needs reformatting, not just a rename.",
    ),
    Rule(
        "st_transform",
        re.compile(r"\bST_Transform\s*\(", re.I),
        None,
        "DuckDB's ST_Transform needs always_xy handling and 'EPSG:xxxx' strings rather "
        "than bare SRID integers - check axis order and argument format at each call site.",
    ),
    Rule(
        "regexp_match",
        re.compile(r"\bREGEXP_MATCH(ES)?\s*\(", re.I),
        None,
        "Postgres's REGEXP_MATCH(ES) and DuckDB's REGEXP_MATCHES/REGEXP_EXTRACT differ in "
        "return shape (boolean vs. array vs. extracted group) - check how the result is used.",
    ),
    Rule(
        "similar-to",
        re.compile(r"\bSIMILAR TO\b", re.I),
        None,
        "DuckDB's SIMILAR TO doesn't treat '%' as a LIKE-style wildcard the way postgres "
        "does. Check what the pattern actually relies on before touching it - not every "
        "SIMILAR TO is broken, and not every fix is a plain LIKE (SIMILAR TO also supports "
        "regex alternation '|', which LIKE can't express at all).",
    ),
    Rule(
        "distinct-on",
        re.compile(r"\bDISTINCT ON\s*\(", re.I),
        None,
        "DuckDB doesn't support DISTINCT ON - rewrite as GROUP BY + MIN/MAX, or "
        "ROW_NUMBER() OVER (PARTITION BY ...) filtered to rn = 1.",
    ),
    Rule(
        "hexagongrid",
        re.compile(r"\bST_HexagonGrid\s*\(", re.I),
        None,
        "No DuckDB equivalent. This is usually a tiling performance hack ahead of a "
        "spatial join - DuckDB's spatial join already does bounding-box pruning, so a "
        "direct join (with an RTREE index if needed) is often just as fast.",
    ),
    Rule(
        "estimatedextent",
        re.compile(r"\bST_EstimatedExtent\s*\(", re.I),
        None,
        "No DuckDB equivalent (relies on postgres table statistics). Compute the extent "
        "directly, e.g. ST_Extent_Agg() / ST_Envelope over the actual rows.",
    ),
    Rule(
        "wkb-geometry-column",
        re.compile(r"\bwkb_geometry\b"),
        None,
        "postgres archives (ogr2ogr) name the geometry column wkb_geometry; DuckDB parquet "
        "archives from a geopandas ingest path are usually 'geom' or 'geometry' instead - "
        "check the actual archive schema per dataset rather than assuming.",
    ),
    Rule(
        "bbl-text-cast",
        re.compile(r"\bbbl\s*::\s*text\b", re.I),
        None,
        "If the source column is numeric (some duckdb parquet archives store bbl as "
        "DOUBLE), a plain ::text cast can leave a trailing '.0' that breaks joins - cast "
        "to BIGINT first, e.g. CAST(bbl AS BIGINT)::text.",
    ),
]


def scan_file(path: Path) -> list[tuple[Rule, int, str]]:
    """Return (rule, line_number, line_text) for every match in the file."""
    hits = []
    for i, line in enumerate(path.read_text().splitlines(), start=1):
        for rule in RULES:
            if rule.pattern.search(line):
                hits.append((rule, i, line.strip()))
    return hits


def fix_file(path: Path) -> tuple[str, int]:
    """Apply every rule with a safe replacement. Returns (new_text, num_fixes)."""
    text = path.read_text()
    num_fixes = 0
    for rule in RULES:
        if rule.replacement is None:
            continue
        text, count = rule.pattern.subn(rule.replacement, text)
        num_fixes += count
    return text, num_fixes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="File or directory to scan")
    parser.add_argument(
        "--ext",
        default=".sql,.yml,.yaml",
        help="Comma-separated file extensions to scan (default: .sql,.yml,.yaml)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Apply the safe auto-fixes in place (default: dry run, report only)",
    )
    args = parser.parse_args()

    extensions = {e if e.startswith(".") else f".{e}" for e in args.ext.split(",")}
    files = (
        [args.path]
        if args.path.is_file()
        else sorted(
            p for p in args.path.rglob("*") if p.is_file() and p.suffix in extensions
        )
    )

    total_hits = 0
    total_fixes = 0
    files_changed = 0
    for path in files:
        hits = scan_file(path)
        if not hits:
            continue
        total_hits += len(hits)
        print(f"\n{path}")
        for rule, line_no, line_text in hits:
            marker = "FIX" if rule.replacement is not None else "REVIEW"
            print(f"  [{marker}] {line_no}: {rule.name}")
            print(f"    {line_text}")
            if rule.replacement is None:
                print(f"    -> {rule.note}")

        if args.write:
            new_text, num_fixes = fix_file(path)
            if num_fixes:
                path.write_text(new_text)
                total_fixes += num_fixes
                files_changed += 1

    print(f"\n{total_hits} match(es) across {len(files)} file(s) scanned.")
    if args.write:
        print(f"Applied {total_fixes} safe fix(es) across {files_changed} file(s).")
    else:
        print("Dry run - pass --write to apply the [FIX] items automatically.")


if __name__ == "__main__":
    main()
