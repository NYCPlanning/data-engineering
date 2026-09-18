import geopandas as gpd
import pandas as pd
import pytest

from dcpy.geospatial import compare

GDB_ZIP = "districts_sample.gdb.zip"

# nynta2010: two NYC neighborhood tabulation areas, chosen for contrast - BK95
# (Erasmus) is a single, 58-vertex polygon, while BK99 (park-cemetery-etc-
# Brooklyn) is a genuinely pathological multi-part polygon: 37 parts, ~14,600
# vertices. The point of including BK99 is to make sure row-level diff
# counting is driven by the key column, not by geometry complexity - a
# comparison shouldn't get slower, wronger, or crash just because one row's
# shape is enormous.
NTA_LAYER = "nynta2010"
NTA_KEY = ["NTACode"]
NTA_INSANE_KEY = "BK99"
NTA_SIMPLE_KEY = "BK95"

# nyad: two NY State Assembly districts, picked as the simplest (lowest
# vertex-count) polygons in the source gdb.
AD_LAYER = "nyad"
AD_KEY = ["AssemDist"]


@pytest.fixture
def nta_gdf(utils_resources_path) -> gpd.GeoDataFrame:
    return gpd.read_file(utils_resources_path / GDB_ZIP, layer=NTA_LAYER)


@pytest.fixture
def ad_gdf(utils_resources_path) -> gpd.GeoDataFrame:
    return gpd.read_file(utils_resources_path / GDB_ZIP, layer=AD_LAYER)


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
    of DEFAULT_FLOAT_RTOL specifically for a noisy, many-vertex polygon."""
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1 + 1e-5
    result = compare.row_level_diff(dev, prod, NTA_KEY, _compare_cols(dev, NTA_KEY))
    assert result.modified == 0


def test_shape_area_change_beyond_tolerance_is_flagged(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "Shape_Area"] *= 1.1
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


def test_columns_differ_uses_tolerance_only_for_float_columns():
    dev = pd.DataFrame({"f": [1.0], "s": ["x"]})
    prod = pd.DataFrame({"f": [1.0 + 1e-8], "s": ["x"]})
    diff = compare.columns_differ(dev, prod, ["f", "s"])
    assert not diff.iloc[0]

    prod_str_diff = pd.DataFrame({"f": [1.0], "s": ["y"]})
    diff2 = compare.columns_differ(dev, prod_str_diff, ["f", "s"])
    assert diff2.iloc[0]


def test_row_level_diff_on_disjoint_frames_reports_full_replacement(ad_gdf):
    """No overlap at all: every dev row is dev-only, every prod row is
    prod-only, nothing is "modified" (there's no common row to compare)."""
    dev = ad_gdf[ad_gdf.AssemDist == 43].reset_index(drop=True)
    prod = ad_gdf[ad_gdf.AssemDist == 48].reset_index(drop=True)
    result = compare.row_level_diff(dev, prod, AD_KEY, _compare_cols(dev, AD_KEY))
    assert result == compare.RowLevelDiff(
        only_in_dev=1, only_in_prod=1, modified=0, precise=True
    )
