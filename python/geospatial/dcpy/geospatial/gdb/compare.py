"""Row/column-level comparison between two attribute-matched (Geo)DataFrames.

Built to compare a "dev" and "prod" read of the same GDB layer (e.g. a rebuilt
export vs. a known-good one), but the functions here don't know anything about
GDBs, recipes, or any particular product - they only assume two DataFrames
share a set of key columns that identify a row, and a set of other columns to
compare. Callers own resolving those column lists, fetching the two
DataFrames, and deciding what to do with the result (report it, fail a build,
write a CSV, ...).
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
