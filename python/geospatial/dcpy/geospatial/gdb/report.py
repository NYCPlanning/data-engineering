"""Human-readable, loggable reporting for a dev/prod GDB comparison.

Turns dcpy.geospatial.gdb.compare's structured results (LayerComparison, and
the plain layer-name -> geometry-type listing from
dcpy.geospatial.gdb.fgdb.layer_geometry_types) into log output and a flat,
CSV-ready row set - the parts of "compare two GDBs and report what's
different" that have nothing to do with any one product: formatting a
row/column diff into text, deciding when an area difference is worth
flagging, when to list every flagged column vs. just a count.

Doesn't know what a "known, already-understood" diff is - that's a caller's
own business logic. Pass known=True to add_layer to mark one as such (the
diff is still shown, just prefixed "KNOWN:" instead of reading like a fresh
problem every run). A caller with its own per-column annotations (e.g. "this
column is known to be unimplemented") should apply those directly to
comparison.column_stats before calling add_layer - they'll show up in both
the log output and the CSV rows as-is.
"""

from dataclasses import dataclass

from dcpy.geospatial.gdb import compare
from dcpy.utils.logging import logger

# Area comparison only makes sense for polygon layers (a line/point layer's
# area is always zero) - callers pass is_polygon=False for those, so this
# threshold only ever applies where it's meaningful.
DEFAULT_AREA_PCT_THRESHOLD = 0.5

# Above this many flagged columns, name a count instead of listing every
# column - a layer with dozens of null-rate anomalies would otherwise
# produce an unreadable one-line note.
DEFAULT_FLAGGED_COLUMNS_LISTED = 5


@dataclass
class LayerReport:
    layer: str
    comparison: compare.LayerComparison
    dev_row_count: int
    prod_row_count: int
    note: str
    flagged_columns: list[str]

    @property
    def is_clean(self) -> bool:
        return self.note == "OK"


class GdbComparisonReport:
    """Accumulates one dev/prod GDB comparison's layer results, logging each
    as it's added, and renders a final per-(layer, column) CSV-ready row set
    for the whole run."""

    def __init__(
        self,
        *,
        area_pct_threshold: float = DEFAULT_AREA_PCT_THRESHOLD,
        flagged_columns_listed: int = DEFAULT_FLAGGED_COLUMNS_LISTED,
    ):
        self.area_pct_threshold = area_pct_threshold
        self.flagged_columns_listed = flagged_columns_listed
        self.dev_layers: dict[str, str | None] = {}
        self.prod_layers: dict[str, str | None] = {}
        self.layers: list[LayerReport] = []

    def log_settings(self, blank_as_null: bool) -> None:
        mode = (
            "NULL and whitespace-only strings treated as equivalent"
            if blank_as_null
            else "STRICT: NULL and whitespace-only strings treated as distinct"
        )
        logger.info(f"=== NULL/BLANK MODE: {mode} ===")

    def log_layer_structure(
        self, dev_layers: dict[str, str | None], prod_layers: dict[str, str | None]
    ) -> None:
        """Log the dev/prod layer listing (name + geometry type, MISMATCH
        flagged) - the first thing a reader wants to know is whether the two
        GDBs even have the same layers. Also records dev_layers/prod_layers
        so common_layers can be derived from them."""
        self.dev_layers = dev_layers
        self.prod_layers = prod_layers
        logger.info("=== LAYER STRUCTURE ===")
        for layer in sorted(set(dev_layers) | set(prod_layers)):
            d = dev_layers.get(layer, "MISSING")
            p = prod_layers.get(layer, "MISSING")
            match = "  *** MISMATCH" if d != p else ""
            logger.info(f"  {layer:20s}  dev={d!s:20s}  prod={p!s}{match}")

    @property
    def common_layers(self) -> list[str]:
        return sorted(set(self.dev_layers) & set(self.prod_layers))

    @property
    def only_in_dev_layers(self) -> list[str]:
        return sorted(set(self.dev_layers) - set(self.prod_layers))

    @property
    def only_in_prod_layers(self) -> list[str]:
        return sorted(set(self.prod_layers) - set(self.dev_layers))

    def add_missing_layer(
        self, layer: str, dev_row_count: int, prod_row_count: int
    ) -> LayerReport:
        """Record a layer that exists on only one side - not a row-level
        diff (there's nothing on the other side to diff against), a
        structural one. A caller that only calls add_layer for
        common_layers never calls it at all for a layer like this, which
        otherwise vanishes from the report entirely rather than showing up
        as the maximum possible discrepancy - downstream, a missing row
        reads as "zero known diffs", not "entirely missing".

        Exactly one of dev_row_count/prod_row_count should be 0 - the real
        count (from whichever side actually has the layer), 0 for the side
        that doesn't.
        """
        if dev_row_count and prod_row_count:
            raise ValueError(
                "add_missing_layer is for a layer absent from one side - "
                "both row counts are nonzero"
            )
        missing_side = "prod" if prod_row_count == 0 else "dev"
        present_count = dev_row_count or prod_row_count
        note = (
            f"MISSING FROM {missing_side.upper()} "
            f"({present_count:,} rows on the other side)"
        )
        entry = LayerReport(
            layer=layer,
            comparison=compare.LayerComparison(
                structure=compare.StructureDiff(
                    missing_from_dev=[],
                    extra_in_dev=[],
                    columns_match_but_order_differs=False,
                    crs_match=False,
                    common_cols=[],
                    attribute_cols=[],
                ),
                key_cols=[],
                key_was_guessed=False,
                declared_key_rejected=None,
                row_level=compare.RowLevelDiff(
                    only_in_dev=dev_row_count,
                    only_in_prod=prod_row_count,
                    modified=None,
                    precise=False,
                ),
                area=None,
                column_stats=[],
            ),
            dev_row_count=dev_row_count,
            prod_row_count=prod_row_count,
            note=note,
            flagged_columns=[],
        )
        self.layers.append(entry)
        self._log_layer(entry)
        return entry

    def add_layer(
        self,
        layer: str,
        comparison: compare.LayerComparison,
        dev_row_count: int,
        prod_row_count: int,
        known: bool = False,
    ) -> LayerReport:
        """Derive and log one layer's report entry.

        comparison.column_stats should already carry any caller-specific
        per-column annotation (mutate .note directly before calling this) -
        this only adds the layer-level "KNOWN:" prefix (known=True), and
        only when there's actually something to prefix (a clean layer stays
        "OK" either way).
        """
        flagged_columns = [
            s.column
            for s in comparison.column_stats
            if s.note and s.note != "spatial" and "KNOWN:" not in s.note
        ]
        note = self._note(comparison, flagged_columns)
        if known and note != "OK":
            note = f"KNOWN: {note}"

        entry = LayerReport(
            layer=layer,
            comparison=comparison,
            dev_row_count=dev_row_count,
            prod_row_count=prod_row_count,
            note=note,
            flagged_columns=flagged_columns,
        )
        self.layers.append(entry)
        self._log_layer(entry)
        return entry

    def _note(self, c: compare.LayerComparison, flagged_columns: list[str]) -> str:
        """One consolidated, human-readable note - structure, keyed
        row-level diff, (for polygons) area, and any per-column anomaly,
        combined - rather than needing to scan every column's note to tell
        whether a layer is actually fine."""
        flags = []
        if c.structure.missing_from_dev:
            flags.append(f"missing {c.structure.missing_from_dev}")
        if c.structure.extra_in_dev:
            flags.append(f"extra {c.structure.extra_in_dev}")
        if c.structure.columns_match_but_order_differs:
            flags.append("column ORDER differs")

        row_level = c.row_level
        total_diff = (
            row_level.only_in_dev + row_level.only_in_prod + (row_level.modified or 0)
        )
        if total_diff:
            if row_level.precise:
                flags.append(
                    f"{total_diff:,} rows differ ({row_level.modified:,} modified, "
                    f"{row_level.only_in_dev:,} dev-only, "
                    f"{row_level.only_in_prod:,} prod-only)"
                )
            else:
                flags.append(
                    f"{total_diff:,} rows differ ({row_level.only_in_dev:,} dev-only, "
                    f"{row_level.only_in_prod:,} prod-only)"
                )

        if c.area is not None and abs(c.area.pct_diff) > self.area_pct_threshold:
            flags.append(f"area {c.area.pct_diff:+.2f}%")
        if len(flagged_columns) > self.flagged_columns_listed:
            flags.append(f"{len(flagged_columns)} columns flagged (see per-column CSV)")
        elif flagged_columns:
            flags.append(f"columns {flagged_columns}")
        return "; ".join(flags) or "OK"

    def _log_layer(self, entry: LayerReport) -> None:
        c = entry.comparison
        crs_ok = "OK" if c.structure.crs_match else "MISMATCH"
        area_str = f"  area={c.area.pct_diff:+7.3f}%" if c.area is not None else ""
        key_str = f"  key=[{', '.join(c.key_cols)}]" if c.key_cols else ""
        logger.info(
            f"=== LAYER: {entry.layer} ===  dev={entry.dev_row_count:,}  "
            f"prod={entry.prod_row_count:,}  crs=[{crs_ok}]{key_str}{area_str}  "
            f"{entry.note}"
        )
        logger.info(
            f"  {'column':30s}  {'dev_nulls%':>10}  {'prod_nulls%':>11}  "
            f"{'dev_nunique':>11}  {'prod_nunique':>12}  note"
        )
        for s in c.column_stats:
            logger.info(
                f"  {s.column:30s}  {s.dev_null_pct:>9.1f}%  {s.prod_null_pct:>10.1f}%"
                f"  {s.dev_nunique:>11,}  {s.prod_nunique:>12,}  {s.note}"
            )

    def rows(self) -> list[dict]:
        """One dict per (layer, column) across every layer added so far -
        ready to write straight to a CSV with csv.DictWriter.

        A layer added via add_missing_layer has no column_stats at all (no
        column-level comparison is possible against a side that doesn't
        exist) - it still gets exactly one row here, with blank per-column
        fields, so the layer's row-level counts and note reach the CSV. A
        caller like build_diffs_report.py keys off "the first row for each
        layer" for its layer-level fields; without this, a layer with empty
        column_stats would contribute zero rows and silently disappear from
        every downstream report instead of surfacing as fully undiffable.
        """
        out = []
        for entry in self.layers:
            c = entry.comparison
            row_diff = entry.dev_row_count - entry.prod_row_count
            for s in c.column_stats or [None]:
                out.append(
                    {
                        "layer": entry.layer,
                        "column": s.column if s else "",
                        "dev_row_count": entry.dev_row_count,
                        "prod_row_count": entry.prod_row_count,
                        "row_diff": row_diff,
                        "dev_null_pct": round(s.dev_null_pct, 2) if s else "",
                        "prod_null_pct": round(s.prod_null_pct, 2) if s else "",
                        "null_pct_diff": (
                            round(s.dev_null_pct - s.prod_null_pct, 2) if s else ""
                        ),
                        "dev_nunique": s.dev_nunique if s else "",
                        "prod_nunique": s.prod_nunique if s else "",
                        "dev_area": (
                            round(c.area.dev_area) if c.area is not None else ""
                        ),
                        "prod_area": (
                            round(c.area.prod_area) if c.area is not None else ""
                        ),
                        "area_pct_diff": (
                            round(c.area.pct_diff, 4) if c.area is not None else ""
                        ),
                        "key_columns": ", ".join(c.key_cols),
                        "key_precise": c.row_level.precise,
                        "rows_only_in_dev": c.row_level.only_in_dev,
                        "rows_only_in_prod": c.row_level.only_in_prod,
                        "rows_modified": (
                            c.row_level.modified
                            if c.row_level.modified is not None
                            else ""
                        ),
                        "note": s.note if s else entry.note,
                        "layer_note": entry.note,
                    }
                )
        return out

    @property
    def clean_layer_count(self) -> int:
        return sum(1 for entry in self.layers if entry.is_clean)

    def log_summary(self) -> None:
        logger.info(f"{self.clean_layer_count}/{len(self.layers)} layers clean")
