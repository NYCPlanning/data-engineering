/*
DESCRIPTION:
    Map KPDB data into final schema for export.
    Translate array from _review_dob to string for export.

INPUTS:
    _kpdb
    _review_dob

OUTPUTS:
    kpdb
    kpdb_deduplicated
    review_no_geometry
    review_dob
*/
DROP TABLE IF EXISTS kpdb;
SELECT
    project_id,
    source,
    record_id,
    record_name,
    borough,
    status,
    type,
    date,
    date_type,
    units_gross,
    units_net,
    has_project_phasing,
    has_future_units,
    prop_within_5_years,
    prop_5_to_10_years,
    prop_after_10_years,
    within_5_years,
    from_5_to_10_years,
    after_10_years,
    phasing_rationale,
    phasing_known,
    classb,
    nycha,
    senior_housing,
    inactive,
    geometry
INTO kpdb
FROM _kpdb WHERE geometry IS NOT NULL;

DROP TABLE IF EXISTS review_no_geometry;
SELECT * INTO review_no_geometry FROM _kpdb WHERE geometry IS NULL;

DROP TABLE IF EXISTS kpdb_deduplicated;
SELECT DISTINCT ON (record_id)
    project_id,
    source,
    record_id,
    record_name,
    borough,
    status,
    type,
    date,
    date_type,
    units_gross,
    units_net,
    has_project_phasing,
    has_future_units,
    prop_within_5_years,
    prop_5_to_10_years,
    prop_after_10_years,
    within_5_years,
    from_5_to_10_years,
    after_10_years,
    phasing_rationale,
    phasing_known,
    classb,
    nycha,
    senior_housing,
    inactive,
    geometry
INTO kpdb_deduplicated
FROM kpdb;

DROP TABLE IF EXISTS review_dob;
SELECT
    v,
    source,
    record_id,
    record_name,
    status,
    type,
    units_gross,
    date,
    date_type,
    inactive,
    no_classa,
    classa_init,
    classa_prop,
    otherb_init,
    otherb_prop,
    date_filed,
    date_lastupdt,
    date_complete,
    dob_multimatch,
    project_has_dob_multi,
    no_geom,
    geom AS geometry,
    array_to_string(project_record_ids, ',') AS project_record_ids
INTO review_dob
FROM _review_dob;
