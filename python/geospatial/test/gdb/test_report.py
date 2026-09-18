from dcpy.geospatial.gdb import compare, report

from .conftest import AD_KEY, NTA_INSANE_KEY, NTA_KEY, NTA_SIMPLE_KEY


def test_add_layer_on_identical_frames_is_clean(nta_gdf):
    r = report.GdbComparisonReport()
    result = compare.compare_layer(nta_gdf, nta_gdf.copy(), declared_key=NTA_KEY)
    entry = r.add_layer("nynta2010", result, len(nta_gdf), len(nta_gdf))

    assert entry.note == "OK"
    assert entry.is_clean
    assert entry.flagged_columns == []
    assert r.clean_layer_count == 1


def test_add_layer_reports_added_removed_and_modified_rows(nta_gdf):
    dev = nta_gdf[nta_gdf.NTACode != "QN99"].reset_index(drop=True)
    prod = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True).copy()
    prod.loc[prod.NTACode == NTA_SIMPLE_KEY, "NTAName"] = "changed name"

    r = report.GdbComparisonReport()
    result = compare.compare_layer(dev, prod, declared_key=NTA_KEY)
    entry = r.add_layer("nynta2010", result, len(dev), len(prod))

    assert not entry.is_clean
    assert "1 modified" in entry.note
    assert "1 dev-only" in entry.note
    assert r.clean_layer_count == 0


def test_add_layer_prefixes_known_diffs_but_leaves_clean_layers_alone(nta_gdf):
    dev = nta_gdf
    prod = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True)

    r = report.GdbComparisonReport()
    result = compare.compare_layer(dev, prod, declared_key=NTA_KEY)

    known_entry = r.add_layer("nynta2010", result, len(dev), len(prod), known=True)
    assert known_entry.note.startswith("KNOWN: ")

    clean_result = compare.compare_layer(dev, dev.copy(), declared_key=NTA_KEY)
    clean_entry = r.add_layer("nyad", clean_result, len(dev), len(dev), known=True)
    # known=True on an otherwise-clean layer shouldn't manufacture a "KNOWN:"
    # note out of nothing.
    assert clean_entry.note == "OK"


def test_add_layer_flags_area_diff_above_threshold(nta_gdf):
    prod = nta_gdf.copy()
    prod.loc[prod.NTACode == NTA_INSANE_KEY, "geometry"] = prod.loc[
        prod.NTACode == NTA_INSANE_KEY, "geometry"
    ].scale(2, 2)

    r = report.GdbComparisonReport(area_pct_threshold=0.1)
    result = compare.compare_layer(nta_gdf, prod, declared_key=NTA_KEY, is_polygon=True)
    entry = r.add_layer("nynta2010", result, len(nta_gdf), len(prod))

    assert "area" in entry.note


def test_add_layer_summarizes_many_flagged_columns_as_a_count(nta_gdf):
    dev = nta_gdf.copy()
    prod = nta_gdf.copy()
    for col in ["BoroName", "CountyFIPS", "NTAName"]:
        dev.loc[dev.NTACode == NTA_SIMPLE_KEY, col] = None

    r = report.GdbComparisonReport(flagged_columns_listed=2)
    result = compare.compare_layer(dev, prod, declared_key=NTA_KEY)
    entry = r.add_layer("nynta2010", result, len(dev), len(prod))

    assert len(entry.flagged_columns) == 3
    assert "columns flagged" in entry.note
    assert "BoroName" not in entry.note  # named columns only shown under the limit


def test_add_layer_respects_caller_overridden_column_notes(nta_gdf):
    """A caller (e.g. a KNOWN_NULL_COLUMNS overlay) can mutate
    comparison.column_stats notes before calling add_layer - those columns
    should be excluded from flagged_columns like any other "KNOWN:" note."""
    dev = nta_gdf.copy()
    dev["NTAName"] = None
    result = compare.compare_layer(dev, nta_gdf, declared_key=NTA_KEY)
    for s in result.column_stats:
        if s.column == "NTAName":
            s.note = "KNOWN: unimplemented (hardcoded null)"

    r = report.GdbComparisonReport()
    entry = r.add_layer("nynta2010", result, len(dev), len(nta_gdf))

    assert "NTAName" not in entry.flagged_columns


def test_log_layer_structure_sets_common_layers():
    r = report.GdbComparisonReport()
    r.log_layer_structure(
        {"nynta2010": "MultiPolygon", "nyad": "MultiPolygon"},
        {"nynta2010": "MultiPolygon", "extra_prod_layer": "Point"},
    )
    assert r.common_layers == ["nynta2010"]


def test_rows_produces_one_dict_per_layer_column(nta_gdf, ad_gdf):
    r = report.GdbComparisonReport()
    nta_result = compare.compare_layer(
        nta_gdf, nta_gdf.copy(), declared_key=NTA_KEY, is_polygon=True
    )
    r.add_layer("nynta2010", nta_result, len(nta_gdf), len(nta_gdf))
    ad_result = compare.compare_layer(ad_gdf, ad_gdf.copy(), declared_key=AD_KEY)
    r.add_layer("nyad", ad_result, len(ad_gdf), len(ad_gdf))

    rows = r.rows()
    assert len(rows) == len(nta_gdf.columns) + len(ad_gdf.columns)
    assert {row["layer"] for row in rows} == {"nynta2010", "nyad"}
    nta_row = next(row for row in rows if row["layer"] == "nynta2010")
    assert nta_row["key_columns"] == "NTACode"
    assert nta_row["rows_only_in_dev"] == 0
    assert nta_row["area_pct_diff"] == 0.0

    ad_row = next(row for row in rows if row["layer"] == "nyad")
    # nyad wasn't compared with is_polygon=True, so no area columns.
    assert ad_row["area_pct_diff"] == ""


def test_log_summary_and_clean_layer_count_track_multiple_layers(nta_gdf, ad_gdf):
    r = report.GdbComparisonReport()
    clean = compare.compare_layer(ad_gdf, ad_gdf.copy(), declared_key=AD_KEY)
    r.add_layer("nyad", clean, len(ad_gdf), len(ad_gdf))

    dirty_prod = nta_gdf[nta_gdf.NTACode != NTA_INSANE_KEY].reset_index(drop=True)
    dirty = compare.compare_layer(nta_gdf, dirty_prod, declared_key=NTA_KEY)
    r.add_layer("nynta2010", dirty, len(nta_gdf), len(dirty_prod))

    assert r.clean_layer_count == 1
    assert len(r.layers) == 2
