/*
Alphabetize QAQC table for export
*/

DROP TABLE IF EXISTS FINAL_qaqc;
SELECT 
    a.job_number,
    a.b_likely_occ_desc,
    a.b_large_alt_reduction,
    a.b_nonres_with_units,
    a.units_co_prop_mismatch,
    a.partially_complete,
    a.units_init_null,
    a.units_prop_null,
    a.units_res_accessory,
    a.outlier_demo_20plus,
    a.outlier_nb_500plus,
    a.outlier_top_alt_increase,
    a.dup_bbl_address_units,
    a.dup_bbl_address,
    a.inactive_with_update,
    a.no_work_job,
    b.geo_water,
    b.geo_taxlot,
    b.geo_null_latlong,
    b.geo_null_boundary,
    a.invalid_date_filed,
    a.invalid_date_lastupdt,
    a.invalid_date_statusd,
    a.invalid_date_statusp,
    a.invalid_date_statusr,
    a.invalid_date_statusx,
    a.incomp_tract_home,
    a.dem_nb_overlap
INTO FINAL_qaqc
FROM MID_qaqc a
LEFT JOIN GEO_qaqc b
ON a.job_number = b.job_number;