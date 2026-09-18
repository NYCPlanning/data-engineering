"""Row/column/layer-level comparison between two attribute-matched (Geo)DataFrames.

Built to compare a "dev" and "prod" read of the same GDB layer (e.g. a rebuilt
export vs. a known-good one), but the functions here don't know anything about
GDB files, recipes, or any particular product - they operate purely on
already-loaded (Geo)DataFrames. Callers own reading those DataFrames (see
dcpy.geospatial.gdb.fgdb for listing/reading GDB layers), resolving key/compare
column lists, and deciding what to do with a result (report it, fail a build,
write a CSV, ...).

Layered, composable rather than one all-in-one entry point, so a caller only
pays for what it needs:
  - compare_layer: the whole thing for one layer - structure, key
    resolution (declared-or-guessed), row-level diff, area, and per-column
    stats, bundled into one LayerComparison. What most callers want.
  - structure_diff / area_diff / column_stats: the layer-level pieces
    compare_layer is built from - column set, CRS, polygon area, and
    per-column null-rate/nunique comparisons - for a caller that wants only
    one of them.
  - row_level_diff: row-level - which rows are add/removed/modified, given a
    resolved key.
  - stringify / columns_differ / composite_key / etc.: the row/column-level
    primitives those are built from.
"""

import uuid
from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass

import numpy as np
import pandas as pd

_NA_SENTINEL = "\x00NA\x00"

# GDAL/pyproj recompute geometry-derived floats (e.g. SHAPE_Length/SHAPE_Area)
# from scratch on read, so two reads of the same geometry can differ in the
# last handful of significant digits even when nothing real changed. These are
# defaults, not universal truths - a layer with noisier geometry (many
# vertices, complex coastlines) can need a looser tolerance; pass rtol/atol to
# override per call. They only apply to columns named in a tolerant_float_cols
# argument (see columns_differ) - never to every float column, or a real
# change in an ordinary float attribute could be swallowed by a tolerance that
# was never meant for it.
DEFAULT_FLOAT_RTOL = 5e-4
DEFAULT_FLOAT_ATOL = 5e-4

# Field names (lowercased for a case-insensitive match) that OpenFileGDB/ArcGIS
# always populate as GDAL-recomputed geometry measures, never real source data -
# "Shape_Length"/"Shape_Area" from OpenFileGDB, "SHAPE_Length" from some ArcGIS
# exports. This is a FileGDB format convention, not a product-specific one, so
# compare_layer treats these as tolerant floats by default - pass
# tolerant_float_cols explicitly to override.
DEFAULT_GEOMETRY_DERIVED_FLOAT_COLUMNS = {"shape_length", "shape_area"}


def stringify(
    df: pd.DataFrame, cols: list[str], blank_as_null: bool = True
) -> pd.DataFrame:
    """String-ify columns for keying/comparison.

    Four normalizations, all aimed at not fooling ourselves into reporting a
    diff that isn't real:
      - Whole-number float columns (e.g. an ID column coming back from pyogrio
        as 9057546.0 instead of an int) are cast to a nullable integer type
        first, so they stringify as "9057546" and compare equal to an
        int-typed twin holding the same value instead of mismatching on every
        single row.
      - Timezone-aware datetime columns are converted to UTC and made naive
        first, so a UTC-aware "2025-08-13 13:03:19+00:00" stringifies the same
        as a naive "2025-08-13 13:03:19" instead of mismatching on every row -
        FileGDB dates have no zone concept, so this is purely a read-side
        artifact, not a real difference.
      - If blank_as_null (the default): whitespace-only string columns are
        blanked to NULL before the sentinel fill. FileGDB's convention for "no
        value" in a text field is often a blank/whitespace string rather than
        a true NULL; whether to adopt that convention varies by column and
        product, so pass blank_as_null=False to see the raw, unnormalized
        comparison instead.
      - Nulls are filled with a sentinel after each column's own astype(str),
        one column at a time - not via a single DataFrame-wide fillna after
        combining columns, which raises if any column is still a numeric
        dtype (Int64 rejects a string fill value) - and not before astype(str)
        either: pandas' string dtype (default since pandas 3.0) makes
        astype(str) preserve NaN as a true null rather than the old behavior
        of stringifying it to "nan", so "|".join would choke on the leftover
        float, and two genuinely-null cells would otherwise compare as
        "different" (float NaN is never equal to itself).

    This is for ATTRIBUTE VALUE comparison, where "two nulls are equal" is the
    right call. composite_key below has different needs (a null key isn't a
    value that should ever compare equal to another null key) and does not
    reuse this sentinel behavior for that reason.
    """
    out = {}
    for col in cols:
        s = df[col]
        if pd.api.types.is_float_dtype(s):
            non_null = s.dropna()
            if not non_null.empty and (non_null % 1 == 0).all():
                s = s.astype("Int64")
        elif isinstance(s.dtype, pd.DatetimeTZDtype):
            s = s.dt.tz_convert("UTC").dt.tz_localize(None)
        elif blank_as_null and pd.api.types.is_string_dtype(s):
            s = s.mask(s.str.strip() == "", None)
        out[col] = s.astype(str).fillna(_NA_SENTINEL)
    return pd.DataFrame(out, index=df.index)


def effective_isna(s: pd.Series, blank_as_null: bool = True) -> pd.Series:
    """True nulls, plus (if blank_as_null) whitespace-only strings - see
    stringify's docstring for why FileGDB's blank-string convention needs to
    count as "no value" too, and why that's togglable."""
    isna = s.isna()
    if blank_as_null and pd.api.types.is_string_dtype(s):
        return isna | (s.fillna("x").str.strip() == "")
    return isna


def composite_key(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    """Build each row's identity from key_cols.

    Two things this deliberately does NOT do, both to avoid a key silently
    misidentifying rows:
      - It ignores FileGDB's blank/whitespace-as-null convention. That
        normalization (see stringify's blank_as_null) exists so "no value"
        text compares equal for ATTRIBUTE values - it has nothing to do with
        row identity, so a key of "" and a key of "   " must stay distinct
        rather than silently becoming "the same row."
      - It never joins key parts into a single string. "|".join(["X|Y", "Z"])
        == "|".join(["X", "Y|Z"]) - two genuinely different key tuples can
        collide into the same joined string if any part happens to contain
        the separator. Returning a tuple per row instead makes that
        collision impossible: tuples compare element-wise, never as
        concatenated text.

    A row with a NULL in any key column can't be identified at all. Rather
    than let it silently collide with every other NULL-keyed row - its own
    side's, or the other side's, which a fixed sentinel would do - each gets
    a one-off identity unique to this call, so it's always counted as
    unmatched (only_in_dev/only_in_prod) instead of being coincidentally
    paired with an unrelated row that also happens to be missing a key.
    """
    if df.empty:
        return pd.Series([], dtype=object)
    stringified = stringify(df, key_cols, blank_as_null=False)
    keys = pd.Series(
        list(stringified.itertuples(index=False, name=None)), index=df.index
    )
    has_null = pd.Series(False, index=df.index)
    for col in key_cols:
        has_null |= df[col].isna()
    if has_null.any():
        # One salt per call, not per row: enough to guarantee dev's null-keyed
        # rows can never coincide with prod's (they come from separate calls,
        # each with its own salt), while the row position makes null-keyed
        # rows distinct from each other within the same call.
        salt = uuid.uuid4().hex
        keys = keys.mask(
            has_null, [f"{_NA_SENTINEL}{salt}:{pos}" for pos in range(len(df))]
        )
    return keys


def columns_differ(
    dev: pd.DataFrame,
    prod: pd.DataFrame,
    cols: list[str],
    blank_as_null: bool = True,
    tolerant_float_cols: Collection[str] = (),
    rtol: float = DEFAULT_FLOAT_RTOL,
    atol: float = DEFAULT_FLOAT_ATOL,
) -> pd.Series:
    """Row-wise: does ANY of these columns differ between dev and prod?

    Only columns named in tolerant_float_cols get the relative+absolute float
    tolerance - see DEFAULT_FLOAT_RTOL/DEFAULT_FLOAT_ATOL for why it exists
    (GDAL/pyproj recompute noise on geometry-derived measures like
    Shape_Length/Shape_Area). Every other column, float or not, is compared
    exactly via stringify - an ordinary float attribute (a rate, a score, a
    measured value) that isn't named here gets exact comparison like any
    other column, so a real small-magnitude change can't be silently absorbed
    by a tolerance that was never meant for it.
    """
    if not cols:
        return pd.Series(False, index=dev.index)
    diff = pd.Series(False, index=dev.index)
    for col in cols:
        dev_s, prod_s = dev[col], prod[col]
        if (
            col in tolerant_float_cols
            and pd.api.types.is_float_dtype(dev_s)
            and pd.api.types.is_float_dtype(prod_s)
        ):
            close = np.isclose(
                dev_s.to_numpy(dtype=float),
                prod_s.to_numpy(dtype=float),
                rtol=rtol,
                atol=atol,
                equal_nan=True,
            )
            diff |= ~close
        else:
            dev_str = stringify(dev, [col], blank_as_null)[col]
            prod_str = stringify(prod, [col], blank_as_null)[col]
            diff |= dev_str.to_numpy() != prod_str.to_numpy()
    return diff


def guess_key_columns(
    dev_df: pd.DataFrame, prod_df: pd.DataFrame, candidate_cols: list[str]
) -> list[str]:
    """Fallback only, for when the caller has no declared key for a layer: a
    single column unique in both dev and prod if one exists, otherwise every
    candidate column together as the row's identity.

    Data-dependent by nature: which column (if any) comes back unique can
    change from run to run. A caller that knows the real key for a layer
    should pass it directly to row_level_diff instead of relying on this -
    it exists only so an unrecognized layer isn't silently skipped.
    """
    for col in candidate_cols:
        if dev_df[col].nunique(dropna=False) == len(dev_df) and prod_df[col].nunique(
            dropna=False
        ) == len(prod_df):
            return [col]
    return candidate_cols


@dataclass
class RowLevelDiff:
    only_in_dev: int
    only_in_prod: int
    # None when precise is False: with a non-unique key, two rows can never be
    # detected as "the same row, modified" - see row_level_diff's docstring.
    modified: int | None
    precise: bool


def row_level_diff(
    dev_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    key_cols: list[str],
    compare_cols: list[str],
    blank_as_null: bool = True,
    tolerant_float_cols: Collection[str] = (),
    rtol: float = DEFAULT_FLOAT_RTOL,
    atol: float = DEFAULT_FLOAT_ATOL,
) -> RowLevelDiff:
    """Keyed row-level diff: how many rows exist only in dev, only in prod, or
    on both sides but with a differing attribute value.

    "modified" is only meaningful when key_cols uniquely identifies a row on
    both sides (precise=True) - if key_cols doesn't uniquely identify rows,
    two rows can never differ while sharing a key by construction, so every
    real disagreement shows up as an add+remove pair instead, and this falls
    back to a duplicate-tolerant multiset comparison (counts, not row-for-row
    pairing).

    tolerant_float_cols controls which compare_cols (if any) get a fuzzy
    float comparison instead of an exact one - see columns_differ. Row
    identity (key_cols) is always exact, regardless of blank_as_null: see
    composite_key.
    """
    dev_keys = composite_key(dev_df, key_cols)
    prod_keys = composite_key(prod_df, key_cols)
    precise = dev_keys.is_unique and prod_keys.is_unique

    if not precise:
        dev_counts = Counter(dev_keys)
        prod_counts = Counter(prod_keys)
        only_in_dev = sum(
            max(c - prod_counts.get(k, 0), 0) for k, c in dev_counts.items()
        )
        only_in_prod = sum(
            max(c - dev_counts.get(k, 0), 0) for k, c in prod_counts.items()
        )
        return RowLevelDiff(
            only_in_dev=only_in_dev,
            only_in_prod=only_in_prod,
            modified=None,
            precise=False,
        )

    dev_indexed = dev_df.set_index(dev_keys)
    prod_indexed = prod_df.set_index(prod_keys)
    dev_only_index = dev_indexed.index.difference(prod_indexed.index)
    prod_only_index = prod_indexed.index.difference(dev_indexed.index)
    common = dev_indexed.index.intersection(prod_indexed.index)

    modified = 0
    if len(common) > 0 and compare_cols:
        modified = int(
            columns_differ(
                dev_indexed.loc[common],
                prod_indexed.loc[common],
                compare_cols,
                blank_as_null,
                tolerant_float_cols,
                rtol,
                atol,
            ).sum()
        )

    return RowLevelDiff(
        only_in_dev=len(dev_only_index),
        only_in_prod=len(prod_only_index),
        modified=modified,
        precise=True,
    )


@dataclass
class StructureDiff:
    missing_from_dev: list[str]  # columns present in prod, absent from dev
    extra_in_dev: list[str]  # columns present in dev, absent from prod
    columns_match_but_order_differs: bool
    crs_match: bool
    # Columns common to both sides, in prod's order - "geometry" included for
    # common_cols, excluded from attribute_cols (attribute_cols is what a
    # caller typically wants for key resolution / row-level comparison).
    common_cols: list[str]
    attribute_cols: list[str]


def structure_diff(dev_gdf: pd.DataFrame, prod_gdf: pd.DataFrame) -> StructureDiff:
    """Column-set, column-order, and CRS comparison for one layer."""
    dev_cols = list(dev_gdf.columns)
    prod_cols = list(prod_gdf.columns)
    dev_col_set = set(dev_cols)
    prod_col_set = set(prod_cols)
    missing_from_dev = sorted(prod_col_set - dev_col_set)
    extra_in_dev = sorted(dev_col_set - prod_col_set)
    order_ok = dev_cols == prod_cols
    common_cols = [c for c in prod_cols if c in dev_col_set]
    attribute_cols = [c for c in common_cols if c != "geometry"]
    dev_crs = str(getattr(dev_gdf, "crs", None) or "None")
    prod_crs = str(getattr(prod_gdf, "crs", None) or "None")
    return StructureDiff(
        missing_from_dev=missing_from_dev,
        extra_in_dev=extra_in_dev,
        columns_match_but_order_differs=(
            not order_ok and not missing_from_dev and not extra_in_dev
        ),
        crs_match=dev_crs == prod_crs,
        common_cols=common_cols,
        attribute_cols=attribute_cols,
    )


@dataclass
class AreaDiff:
    dev_area: float
    prod_area: float
    pct_diff: float


def area_diff(dev_gdf: pd.DataFrame, prod_gdf: pd.DataFrame) -> AreaDiff:
    """Total polygon area on each side and the relative difference. Only
    meaningful for polygon layers - callers should gate this on knowing the
    layer is a polygon layer (a line/point layer's area is always zero)."""
    dev_area = dev_gdf.geometry.area.sum()
    prod_area = prod_gdf.geometry.area.sum()
    pct_diff = (dev_area - prod_area) / prod_area * 100 if prod_area else 0.0
    return AreaDiff(dev_area=dev_area, prod_area=prod_area, pct_diff=pct_diff)


@dataclass
class ColumnStats:
    column: str
    dev_null_pct: float
    prod_null_pct: float
    dev_nunique: int
    prod_nunique: int
    all_null_in_dev: bool
    # "spatial" for the geometry column, "ALL NULL in dev", a null-rate-diff
    # message, or "" - a generic note only. A caller with its own conventions
    # (e.g. "this column is known/expected to be all-NULL for now") should
    # treat all_null_in_dev/the raw percentages as the source of truth and
    # overlay its own annotation rather than parse this string.
    note: str


def column_stats(
    dev_gdf: pd.DataFrame,
    prod_gdf: pd.DataFrame,
    cols: list[str],
    blank_as_null: bool = True,
    null_pct_diff_threshold: float = 5.0,
) -> list[ColumnStats]:
    """Per-column null-rate and nunique comparison, for every column in cols
    (pass structure_diff's common_cols to cover every shared column,
    including geometry)."""
    stats = []
    for col in cols:
        dev_s = dev_gdf[col]
        prod_s = prod_gdf[col]
        dev_na = effective_isna(dev_s, blank_as_null)
        prod_na = effective_isna(prod_s, blank_as_null)
        dev_null_pct = dev_na.mean() * 100
        prod_null_pct = prod_na.mean() * 100
        dev_nunique = dev_s[~dev_na].nunique(dropna=True)
        prod_nunique = prod_s[~prod_na].nunique(dropna=True)
        all_null_in_dev = bool(dev_null_pct == 100 and prod_null_pct < 100)

        note = ""
        if col == "geometry":
            note = "spatial"
        elif all_null_in_dev:
            note = "ALL NULL in dev"
        elif abs(dev_null_pct - prod_null_pct) > null_pct_diff_threshold:
            note = f"null rate diff {dev_null_pct - prod_null_pct:+.1f}pp"

        stats.append(
            ColumnStats(
                column=col,
                dev_null_pct=dev_null_pct,
                prod_null_pct=prod_null_pct,
                dev_nunique=dev_nunique,
                prod_nunique=prod_nunique,
                all_null_in_dev=all_null_in_dev,
                note=note,
            )
        )
    return stats


@dataclass
class LayerComparison:
    structure: StructureDiff
    key_cols: list[str]
    # True when no usable declared_key was given (either None, or its columns
    # aren't all present) and guess_key_columns had to pick instead - a
    # caller usually wants to surface this (e.g. a warning) since a guessed
    # key is data-dependent and can silently pick a different column on a
    # different run.
    key_was_guessed: bool
    # The declared_key the caller passed, if it was rejected for not being a
    # subset of the layer's attribute columns - None if no key was declared,
    # or the declared key was used as-is.
    declared_key_rejected: list[str] | None
    row_level: RowLevelDiff
    area: AreaDiff | None  # None unless is_polygon=True
    column_stats: list[ColumnStats]


def compare_layer(
    dev_gdf: pd.DataFrame,
    prod_gdf: pd.DataFrame,
    *,
    declared_key: list[str] | None = None,
    is_polygon: bool = False,
    blank_as_null: bool = True,
    tolerant_float_cols: Collection[str] | None = None,
    rtol: float = DEFAULT_FLOAT_RTOL,
    atol: float = DEFAULT_FLOAT_ATOL,
    null_pct_diff_threshold: float = 5.0,
) -> LayerComparison:
    """Everything for one layer: structure, key resolution, row-level diff,
    (for polygons) area, and per-column stats - the composition every caller
    of the pieces below ends up needing to do anyway.

    Key resolution: declared_key is used as-is if given and every one of its
    columns is actually present; otherwise guess_key_columns picks instead
    (see key_was_guessed/declared_key_rejected on the result - a caller
    usually wants to log this, since a guessed key is data-dependent).

    tolerant_float_cols=None (the default) auto-detects
    DEFAULT_GEOMETRY_DERIVED_FLOAT_COLUMNS among the layer's compare columns
    (case-insensitively) rather than requiring every caller to rediscover
    that FileGDB convention itself - pass an explicit collection (including
    an empty one) to override.
    """
    structure = structure_diff(dev_gdf, prod_gdf)

    key_was_guessed = False
    declared_key_rejected = None
    if not structure.attribute_cols:
        # No non-geometry columns at all - fall back to a plain row-count diff.
        key_cols: list[str] = []
        row_level = RowLevelDiff(
            only_in_dev=max(len(dev_gdf) - len(prod_gdf), 0),
            only_in_prod=max(len(prod_gdf) - len(dev_gdf), 0),
            modified=None,
            precise=False,
        )
    else:
        if declared_key and all(c in structure.attribute_cols for c in declared_key):
            key_cols = declared_key
        else:
            if declared_key:
                declared_key_rejected = declared_key
            key_was_guessed = True
            key_cols = guess_key_columns(dev_gdf, prod_gdf, structure.attribute_cols)

        compare_cols = [c for c in structure.attribute_cols if c not in key_cols]
        resolved_tolerant_float_cols: Collection[str]
        if tolerant_float_cols is None:
            resolved_tolerant_float_cols = {
                c
                for c in compare_cols
                if c.lower() in DEFAULT_GEOMETRY_DERIVED_FLOAT_COLUMNS
            }
        else:
            resolved_tolerant_float_cols = tolerant_float_cols
        row_level = row_level_diff(
            dev_gdf,
            prod_gdf,
            key_cols,
            compare_cols,
            blank_as_null,
            resolved_tolerant_float_cols,
            rtol,
            atol,
        )

    area = area_diff(dev_gdf, prod_gdf) if is_polygon else None
    stats = column_stats(
        dev_gdf, prod_gdf, structure.common_cols, blank_as_null, null_pct_diff_threshold
    )

    return LayerComparison(
        structure=structure,
        key_cols=key_cols,
        key_was_guessed=key_was_guessed,
        declared_key_rejected=declared_key_rejected,
        row_level=row_level,
        area=area,
        column_stats=stats,
    )
