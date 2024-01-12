DROP TABLE IF EXISTS qaqc_app;

SELECT
    hpd_review.job_number,
    hpd_review.b_likely_occ_desc,
    hpd_review.b_large_alt_reduction,
    hpd_review.b_nonres_with_units,
    hpd_review.units_co_prop_mismatch,
    hpd_review.partially_complete,
    hpd_review.units_init_null,
    hpd_review.units_prop_null,
    hpd_review.units_res_accessory,
    hpd_review.outlier_demo_20plus,
    hpd_review.outlier_nb_500plus,
    hpd_review.outlier_top_alt_increase,
    hpd_review.dup_bbl_address_units,
    hpd_review.dup_bbl_address,
    hpd_review.inactive_with_update,
    hpd_review.no_work_job,
    hpd_review.geo_water,
    hpd_review.geo_taxlot,
    hpd_review.geo_null_latlong,
    hpd_review.geo_null_boundary,
    hpd_review.invalid_date_filed,
    hpd_review.invalid_date_lastupdt,
    hpd_review.invalid_date_statusd,
    hpd_review.invalid_date_statusp,
    hpd_review.invalid_date_statusr,
    hpd_review.invalid_date_statusx,
    hpd_review.incomp_tract_home,
    hpd_review.dem_nb_overlap,
    qaqc_app_additions.classa_net_mismatch,
    qaqc_app_additions.manual_hny_match_check,
    qaqc_app_additions.manual_corrections_not_applied
INTO qaqc_app
FROM
    final_qaqc AS hpd_review
LEFT JOIN qaqc_app_additions
    ON hpd_review.job_number = qaqc_app_additions.job_number
WHERE
    hpd_review.b_likely_occ_desc = 1
    OR hpd_review.b_large_alt_reduction = 1
    OR hpd_review.b_nonres_with_units = 1
    OR hpd_review.partially_complete = 1
    OR hpd_review.units_init_null = 1
    OR hpd_review.units_prop_null = 1
    OR hpd_review.units_res_accessory = 1
    OR hpd_review.outlier_demo_20plus = 1
    OR hpd_review.outlier_nb_500plus = 1
    OR hpd_review.outlier_top_alt_increase = 1
    OR hpd_review.inactive_with_update = 1
    OR hpd_review.no_work_job = 1
    OR hpd_review.geo_water = 1
    OR hpd_review.geo_taxlot = 1
    OR hpd_review.geo_null_latlong = 1
    OR hpd_review.geo_null_boundary = 1
    OR hpd_review.invalid_date_filed = 1
    OR hpd_review.invalid_date_lastupdt = 1
    OR hpd_review.invalid_date_statusd = 1
    OR hpd_review.invalid_date_statusp = 1
    OR hpd_review.invalid_date_statusr = 1
    OR hpd_review.invalid_date_statusx = 1
    OR hpd_review.dem_nb_overlap = 1
    OR hpd_review.dup_bbl_address_units IS NOT NULL
    OR hpd_review.dup_bbl_address IS NOT NULL
    OR hpd_review.units_co_prop_mismatch IS NOT NULL
    OR qaqc_app_additions.classa_net_mismatch = 1
    OR qaqc_app_additions.manual_hny_match_check = 1
    OR qaqc_app_additions.manual_corrections_not_applied = 1;
