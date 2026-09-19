import geopandas as gpd
import pandas as pd
import pytest

from dcpy.geospatial.gdb import compare

from .conftest import AD_KEY, NTA_INSANE_KEY, NTA_KEY, NTA_SIMPLE_KEY


def _compare_cols(gdf: gpd.GeoDataFrame, key_cols: list[str]) -> list[str]:
    return [c for c in gdf.columns if c not in key_cols and c != "geometry"]


def test_nta_fixture_has_a_pathological_multipart_geometry(nta_gdf):
    """Sanity-check the fixture itself: BK99 must still be the many-part,
    many-vertex geometry it was selected for, or the tests below wouldn't be
    exercising anything unusual."""
    row = nta_gdf.loc[nta_gdf.NTACode == NTA_INSANE_KEY].iloc[0]
    assert len(row.geometry.geoms) > 30
    n_vertices = sum(
        len(part.exterior.coords) + sum(len(ring.coords) for ring in part.interiors)
        for part in row.geometry.geoms
    )
    assert n_vertices > 10_000


@pytest.mark.parametrize(
    "layer_fixture, key_cols", [("nta_gdf", NTA_KEY), ("ad_gdf", AD_KEY)]
)
def test_identical_frames_have_no_diff(request, layer_fixture, key_cols):
    dev = request.getfixturevalue(layer_fixture)
    prod = dev.copy()
    result = compare.row_level_diff(dev, prod, key_cols, _compare_cols(dev, key_cols))
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=0, precise=True
    )


def test_row_added_in_dev_counts_as_dev_only(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True)
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=0, modified=0, precise=True
    )


def test_row_removed_from_dev_counts_as_prod_only(nta_gdf):
    dev = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True)
    prod = nta_gdf
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=1, modified=0, precise=True
    )


def test_row_removed_on_both_sides_leaves_no_diff(nta_gdf):
    """Dropping the pathological row from BOTH sides should leave a clean
    diff - regression guard against any code path that treats BK99 specially
    (e.g. a key computation that silently breaks on its geometry)."""
    dev = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True)
    prod = dev.copy()
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=0, precise=True
    )


def test_modified_attribute_on_insane_geometry_row_counts_as_modified(nta_gdf):
    """Changing a plain attribute on BK99 (not its geometry) must be reported
    as one modified row, not an add+remove pair."""
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "NTAName"] = "changed name"
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=1, precise=True
    )


def test_modified_attribute_on_simple_geometry_row_counts_as_modified(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_SIMPLE_KEY, "BoroCode"] = 99
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=1, precise=True
    )


def test_add_remove_and_modify_combine_correctly(nta_gdf):
    """One row added, one removed, one modified, all in the same comparison -
    each should be attributed to the right bucket rather than smearing into
    each other."""
    dev = nta_gdf[nta_gdf.NTACode != "QN99"].reset_index(drop=True)
    prod = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True).copy()
    prod.loc[prod.NTACode == NTA_SIMPLE_KEY, "NTAName"] = "changed name"
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=0, modified=1, precise=True
    )


def test_shape_area_noise_within_tolerance_is_not_flagged(nta_gdf):
    """Simulates the real dev/prod scenario this tolerance exists for: the
    same geometry, recomputed, landing a hair off in its last few significant
    digits. BK99's Shape_Area is large and its geometry is complex, so a
    relative perturbation near the tolerance boundary is a meaningful check
    of DEFAULT_FLOAT_RTOL specifically for a noisy, many-vertex polygon.
    Shape_Area must be opted in via tolerant_float_cols - see
    test_float_column_not_opted_in_is_compared_exactly for why that matters."""
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1 + 1e-5
    result = compare.row_level_diff(
        dev,
        prod,
        NTA_KEY,
        _compare_cols(dev, NTA_KEY),
        tolerant_float_cols=["Shape_Area"],
    )
    assert result.modified == 0


def test_shape_area_change_beyond_tolerance_is_flagged(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1.1
    result = compare.row_level_diff(
        dev,
        prod,
        NTA_KEY,
        _compare_cols(dev, NTA_KEY),
        tolerant_float_cols=["Shape_Area"],
    )
    assert result.modified == 1


def test_float_column_not_opted_in_is_compared_exactly(nta_gdf):
    """Regression test: the recompute-noise tolerance used to apply to every
    float column by default. That silently swallowed real changes in ordinary
    float attributes - a tiny Shape_Area-scale perturbation on a column NOT
    named in tolerant_float_cols must now be caught, not absorbed."""
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1 + 1e-5
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result.modified == 1


def test_blank_as_null_treats_null_and_whitespace_as_equal(nta_gdf):
    dev = nta_gdf.copy()
    prod = nta_gdf.copy()
    dev.loc[dev.NTACode == NTA_SIMPLE_KEY, "CountyFIPS"] = None
    prod.loc[prod.NTACode == NTA_SIMPLE_KEY, "CountyFIPS"] = "   "
    result = compare.row_level_diff(
        dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY), blank_as_null=True
    )
    assert result.modified == 0


def test_strict_nulls_treats_null_and_whitespace_as_different(nta_gdf):
    dev = nta_gdf.copy()
    prod = nta_gdf.copy()
    dev.loc[dev.NTACode == NTA_SIMPLE_KEY, "CountyFIPS"] = None
    prod.loc[prod.NTACode == NTA_SIMPLE_KEY, "CountyFIPS"] = "   "
    result = compare.row_level_diff(
        dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY), blank_as_null=False
    )
    assert result.modified == 1


def test_non_unique_key_falls_back_to_imprecise_multiset_diff(nta_gdf):
    """If key_cols doesn't actually identify rows uniquely (e.g. every row
    shares the same BoroCode), row_level_diff can't pair rows up - it should
    report add/remove counts via the duplicate-tolerant fallback instead of
    raising, and "modified" should come back as None rather than a count."""
    dev = nta_gdf[nta_gdf.NTACode.isin([NTA_SIMPLE_KEY, NTA_INSANE_KEY])].reset_index(
        drop=True
    )
    prod = dev.copy()
    # BoroCode is "Brooklyn" (5) for both rows in this subset - not a real key.
    result = compare.row_level_diff(dev, prod, ["BoroCode"], ["NTAName"])
    assert result.precise is False
    assert result.modified is None
    assert result.only_in_dev == 0
    assert result.only_in_prod == 0


def test_guess_key_columns_picks_a_unique_column(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf.copy()
    # BoroCode/BoroName are identical for both fixture rows (both Brooklyn) -
    # only NTACode is actually unique, so it should be the one picked.
    candidates = ["BoroCode", "BoroName", "NTACode"]
    assert compare.guess_key_columns(dev, prod, candidates) == ["NTACode"]


def test_guess_key_columns_falls_back_to_all_candidates_when_none_unique(nta_gdf):
    dev = nta_gdf[["BoroCode", "BoroName"]].copy()
    prod = dev.copy()
    assert compare.guess_key_columns(dev, prod, ["BoroCode", "BoroName"]) == [
        "BoroCode",
        "BoroName",
    ]


def test_effective_isna_treats_whitespace_as_null_by_default():
    s = pd.Series(["a", "   ", None, ""], dtype="string")
    assert list(compare.effective_isna(s)) == [False, True, True, True]
    assert list(compare.effective_isna(s, blank_as_null=False)) == [
        False,
        False,
        True,
        False,
    ]


def test_composite_key_on_empty_dataframe_returns_empty_series():
    empty = pd.DataFrame({"a": pd.Series([], dtype="string")})
    key = compare.composite_key(empty, ["a"])
    assert len(key) == 0


def test_composite_key_does_not_collide_when_a_part_contains_the_join_separator():
    """Regression test: composite_key used to join key parts with "|", so two
    genuinely different multi-column keys could collide into the same string
    ("|".join(["X|Y", "Z"]) == "|".join(["X", "Y|Z"])). A real attribute
    change on one of those rows must still be caught, not silently merged
    into a false match via the imprecise fallback."""
    dev = pd.DataFrame({"k1": ["X|Y", "X"], "k2": ["Z", "Y|Z"], "val": [1, 2]})
    prod = pd.DataFrame({"k1": ["X|Y", "X"], "k2": ["Z", "Y|Z"], "val": [99, 2]})
    keys = compare.composite_key(dev, ["k1", "k2"])
    assert keys.is_unique
    result = compare.row_level_diff(dev, prod, ["k1", "k2"], ["val"])
    assert result == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=1, precise=True
    )


def test_null_keyed_rows_are_never_paired_with_each_other():
    """Regression test: a NULL key used to fill a fixed sentinel, so any two
    NULL-keyed rows (on either side) collided into "the same row" - pairing
    unrelated records instead of counting them as an add and a remove."""
    dev = pd.DataFrame({"id": ["A", None], "val": ["a-value", "dev-unrelated"]})
    prod = pd.DataFrame({"id": ["A", None], "val": ["a-value", "prod-unrelated"]})
    result = compare.row_level_diff(dev, prod, ["id"], ["val"])
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=1, modified=0, precise=True
    )


def test_key_identity_ignores_blank_as_null():
    """Regression test: composite_key used to run key columns through the
    same blank-as-null normalization as attribute values, so a key of ""
    and a key of "   " collapsed into the same identity. Row identity must
    stay exact regardless of blank_as_null."""
    dev = pd.DataFrame({"code": ["A", ""], "val": ["a-value", "dev-record"]})
    prod = pd.DataFrame({"code": ["A", "   "], "val": ["a-value", "prod-record"]})
    result = compare.row_level_diff(dev, prod, ["code"], ["val"], blank_as_null=True)
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=1, modified=0, precise=True
    )


def test_columns_differ_applies_tolerance_only_to_opted_in_float_columns():
    dev = pd.DataFrame({"f": [1.0], "s": ["x"]})
    prod = pd.DataFrame({"f": [1.0 + 1e-8], "s": ["x"]})
    diff = compare.columns_differ(dev, prod, ["f", "s"], tolerant_float_cols=["f"])
    assert not diff.iloc[0]

    prod_str_diff = pd.DataFrame({"f": [1.0], "s": ["y"]})
    diff2 = compare.columns_differ(
        dev, prod_str_diff, ["f", "s"], tolerant_float_cols=["f"]
    )
    assert diff2.iloc[0]


def test_columns_differ_defaults_to_exact_comparison_for_float_columns():
    """No tolerant_float_cols passed: even a tiny float perturbation - well
    within DEFAULT_FLOAT_RTOL/ATOL - must be caught, since nothing opted this
    column into the geometry-noise tolerance."""
    dev = pd.DataFrame({"f": [1.0]})
    prod = pd.DataFrame({"f": [1.0 + 1e-8]})
    diff = compare.columns_differ(dev, prod, ["f"])
    assert diff.iloc[0]


def test_match_columns_matches_same_name_regardless_of_case():
    """Regression test: SHAPE_Area (dev) vs Shape_Area (prod) used to be
    treated as entirely different columns - dropped from every row/column
    comparison instead of being recognized as the same field with a casing
    quirk, silently hiding real content differences."""
    result = compare.match_columns(["SHAPE_Area", "NTACode"], ["Shape_Area", "NTACode"])
    assert result.common == [("NTACode", "NTACode"), ("SHAPE_Area", "Shape_Area")]
    assert result.case_mismatches == [("SHAPE_Area", "Shape_Area")]
    assert result.missing_from_dev == []
    assert result.extra_in_dev == []


def test_match_columns_reports_genuinely_missing_or_extra_columns():
    result = compare.match_columns(["NTACode", "DevOnly"], ["NTACode", "ProdOnly"])
    assert result.common == [("NTACode", "NTACode")]
    assert result.case_mismatches == []
    assert result.missing_from_dev == ["ProdOnly"]
    assert result.extra_in_dev == ["DevOnly"]


def test_row_level_diff_on_disjoint_frames_reports_full_replacement(ad_gdf):
    """No overlap at all: every dev row is dev-only, every prod row is
    prod-only, nothing is "modified" (there's no common row to compare)."""
    dev = ad_gdf[ad_gdf.AssemDist == 43].reset_index(drop=True)
    prod = ad_gdf[ad_gdf.AssemDist == 48].reset_index(drop=True)
    result = compare.row_level_diff(dev, prod, AD_KEY, _compare_cols(dev, AD_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=1, modified=0, precise=True
    )


def test_structure_diff_on_identical_frames_is_clean(nta_gdf):
    result = compare.structure_diff(nta_gdf, nta_gdf.copy())
    assert result.missing_from_dev == []
    assert result.extra_in_dev == []
    assert result.columns_match_but_order_differs is False
    assert result.crs_match is True
    assert set(result.attribute_cols) == {
        "BoroCode",
        "BoroName",
        "CountyFIPS",
        "NTACode",
        "NTAName",
        "Shape_Length",
        "Shape_Area",
    }
    assert "geometry" not in result.attribute_cols
    assert "geometry" in result.common_cols


def test_structure_diff_detects_missing_and_extra_columns(nta_gdf):
    dev = nta_gdf.drop(columns=["BoroName"])
    prod = nta_gdf.drop(columns=["CountyFIPS"])
    result = compare.structure_diff(dev, prod)
    assert result.missing_from_dev == ["BoroName"]
    assert result.extra_in_dev == ["CountyFIPS"]


def test_structure_diff_detects_column_order_difference(nta_gdf):
    reordered = nta_gdf[[*reversed(nta_gdf.columns)]]
    result = compare.structure_diff(nta_gdf, reordered)
    assert result.columns_match_but_order_differs is True
    assert result.missing_from_dev == []
    assert result.extra_in_dev == []


def test_structure_diff_detects_crs_mismatch(nta_gdf):
    reprojected = nta_gdf.to_crs("EPSG:4326")
    result = compare.structure_diff(nta_gdf, reprojected)
    assert result.crs_match is False


def test_area_diff_on_identical_geometry_is_zero(nta_gdf):
    result = compare.area_diff(nta_gdf, nta_gdf.copy())
    assert result.pct_diff == 0.0
    assert result.dev_area == result.prod_area


def test_area_diff_reflects_a_real_area_change(nta_gdf):
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "geometry"] = prod.loc[
        prod.NTACode == NTA_INSANE_KEY, "geometry"
    ].scale(2, 2)
    result = compare.area_diff(nta_gdf, prod)
    assert result.pct_diff < 0  # dev's total area is now smaller than prod's


def test_column_stats_flags_all_null_in_dev(nta_gdf):
    dev = nta_gdf.copy()
    dev["NTAName"] = None
    stats = {s.column: s for s in compare.column_stats(dev, nta_gdf, ["NTAName"])}
    assert stats["NTAName"].all_null_in_dev
    assert stats["NTAName"].note == "ALL NULL in dev"


def test_column_stats_flags_null_rate_diff_above_threshold(nta_gdf):
    dev = nta_gdf.copy()
    dev.loc[dev.NTACode == NTA_SIMPLE_KEY, "CountyFIPS"] = None
    stats = {s.column: s for s in compare.column_stats(dev, nta_gdf, ["CountyFIPS"])}
    # 1 of 2 rows null in dev, 0 in prod -> 50pp diff, comfortably above the
    # default 5pp threshold. Not "ALL NULL in dev" (only 1 of 2 rows is null).
    assert not stats["CountyFIPS"].all_null_in_dev
    assert "null rate diff" in stats["CountyFIPS"].note


def test_column_stats_marks_geometry_column_as_spatial(nta_gdf):
    stats = {
        s.column: s for s in compare.column_stats(nta_gdf, nta_gdf.copy(), ["geometry"])
    }
    assert stats["geometry"].note == "spatial"


def test_column_stats_clean_column_has_no_note(nta_gdf):
    stats = {
        s.column: s for s in compare.column_stats(nta_gdf, nta_gdf.copy(), ["NTACode"])
    }
    assert stats["NTACode"].note == ""
    assert not stats["NTACode"].all_null_in_dev


def test_compare_layer_with_valid_declared_key_uses_it_without_guessing(nta_gdf):
    result = compare.compare_layer(nta_gdf, nta_gdf.copy(), declared_key=NTA_KEY)
    assert result.key_cols == NTA_KEY
    assert result.key_was_guessed is False
    assert result.declared_key_rejected is None
    assert result.row_level == compare.RowLevelDiff(
        only_in_dev=0, only_in_prod=0, modified=0, precise=True
    )


def test_compare_layer_with_no_declared_key_guesses_one(nta_gdf):
    # Both rows given the same Shape_Area/Shape_Length so only NTACode is
    # actually unique - otherwise guess_key_columns could pick either float
    # column first, since the fixture only has 2 rows.
    dev = nta_gdf.copy()
    dev["Shape_Area"] = 1.0
    dev["Shape_Length"] = 1.0
    result = compare.compare_layer(dev, dev.copy(), declared_key=None)
    assert result.key_was_guessed is True
    assert result.declared_key_rejected is None
    assert result.key_cols == ["NTACode"]


def test_compare_layer_with_invalid_declared_key_falls_back_to_guessing(nta_gdf):
    dev = nta_gdf.copy()
    dev["Shape_Area"] = 1.0
    dev["Shape_Length"] = 1.0
    bogus_key = ["NotARealColumn"]
    result = compare.compare_layer(dev, dev.copy(), declared_key=bogus_key)
    assert result.key_was_guessed is True
    assert result.declared_key_rejected == bogus_key
    assert result.key_cols == ["NTACode"]


def test_compare_layer_computes_area_only_when_is_polygon(nta_gdf):
    with_area = compare.compare_layer(nta_gdf, nta_gdf.copy(), is_polygon=True)
    assert with_area.area is not None
    assert with_area.area.pct_diff == 0.0

    without_area = compare.compare_layer(nta_gdf, nta_gdf.copy(), is_polygon=False)
    assert without_area.area is None


def test_compare_layer_defaults_to_tolerant_shape_area_case_insensitively(nta_gdf):
    """No tolerant_float_cols passed: compare_layer should auto-detect
    Shape_Area as a geometry-derived measure (case-insensitively) and absorb
    a tiny recompute-noise-scale perturbation on it by default."""
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1 + 1e-5
    result = compare.compare_layer(nta_gdf, prod, declared_key=NTA_KEY)
    assert result.row_level.modified == 0


def test_compare_layer_still_catches_a_real_shape_area_change_by_default(nta_gdf):
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1.1
    result = compare.compare_layer(nta_gdf, prod, declared_key=NTA_KEY)
    assert result.row_level.modified == 1


def test_compare_layer_default_tolerance_does_not_apply_to_ordinary_floats(nta_gdf):
    """Regression guard: only the recognized geometry-derived measure names
    get the default tolerance - an ordinary float attribute must still be
    compared exactly even without the caller doing anything special."""
    dev = nta_gdf.copy()
    dev["growth_rate"] = [0.0001, 0.05]
    prod = dev.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "growth_rate"] = 0.0006
    result = compare.compare_layer(dev, prod, declared_key=NTA_KEY)
    assert result.row_level.modified == 1


def test_compare_layer_explicit_tolerant_float_cols_overrides_the_default(nta_gdf):
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1 + 1e-5
    result = compare.compare_layer(
        nta_gdf, prod, declared_key=NTA_KEY, tolerant_float_cols=set()
    )
    # Explicitly overridden to an empty set - even Shape_Area is now exact.
    assert result.row_level.modified == 1


def test_compare_layer_populates_column_stats_for_every_common_column(nta_gdf):
    result = compare.compare_layer(nta_gdf, nta_gdf.copy(), declared_key=NTA_KEY)
    assert {s.column for s in result.column_stats} == set(nta_gdf.columns)


def test_compare_layer_structure_reflects_real_column_differences(nta_gdf):
    dev = nta_gdf.drop(columns=["BoroName"])
    result = compare.compare_layer(dev, nta_gdf, declared_key=NTA_KEY)
    assert result.structure.missing_from_dev == ["BoroName"]
