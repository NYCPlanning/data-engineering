DROP TABLE IF EXISTS qaqc_app;

SELECT
    HPD_review.job_number,
    HPD_review.b_likely_occ_desc,
    HPD_review.b_large_alt_reduction,
    HPD_review.b_nonres_with_units,
    HPD_review.units_co_prop_mismatch,
    HPD_review.partially_complete,
    HPD_review.units_init_null,
    HPD_review.units_prop_null,
    HPD_review.units_res_accessory,
    HPD_review.outlier_demo_20plus,
    HPD_review.outlier_nb_500plus,
    HPD_review.outlier_top_alt_increase,
    HPD_review.dup_bbl_address_units,
    HPD_review.dup_bbl_address,
    HPD_review.inactive_with_update,
    HPD_review.no_work_job,
    HPD_review.geo_water,
    HPD_review.geo_taxlot,
    HPD_review.geo_null_latlong,
    HPD_review.geo_null_boundary,
    HPD_review.invalid_date_filed,
    HPD_review.invalid_date_lastupdt,
    HPD_review.invalid_date_statusd,
    HPD_review.invalid_date_statusp,
    HPD_review.invalid_date_statusr,
    HPD_review.invalid_date_statusx,
    HPD_review.incomp_tract_home,
    HPD_review.dem_nb_overlap,
    qaqc_app_additions.classa_net_mismatch,
    qaqc_app_additions.manual_hny_match_check,
    qaqc_app_additions.manual_corrections_not_applied
INTO qaqc_app
FROM
    FINAL_qaqc HPD_review
LEFT JOIN qaqc_app_additions
ON HPD_review.job_number = qaqc_app_additions.job_number
WHERE
    HPD_review.b_likely_occ_desc = 1
    OR HPD_review.b_large_alt_reduction = 1
    OR HPD_review.b_nonres_with_units = 1
    OR HPD_review.partially_complete = 1
    OR HPD_review.units_init_null = 1
    OR HPD_review.units_prop_null = 1
    OR HPD_review.units_res_accessory = 1
    OR HPD_review.outlier_demo_20plus = 1
    OR HPD_review.outlier_nb_500plus = 1
    OR HPD_review.outlier_top_alt_increase = 1
    OR HPD_review.inactive_with_update = 1
    OR HPD_review.no_work_job = 1
    OR HPD_review.geo_water = 1
    OR HPD_review.geo_taxlot = 1
    OR HPD_review.geo_null_latlong = 1
    OR HPD_review.geo_null_boundary = 1
    OR HPD_review.invalid_date_filed = 1
    OR HPD_review.invalid_date_lastupdt = 1
    OR HPD_review.invalid_date_statusd = 1
    OR HPD_review.invalid_date_statusp = 1
    OR HPD_review.invalid_date_statusr = 1
    OR HPD_review.invalid_date_statusx = 1
    OR HPD_review.dem_nb_overlap = 1
    OR HPD_review.dup_bbl_address_units IS NOT NULL
    OR HPD_review.dup_bbl_address IS NOT NULL
    OR HPD_review.units_co_prop_mismatch IS NOT NULL
    OR qaqc_app_additions.classa_net_mismatch = 1
    OR qaqc_app_additions.manual_hny_match_check = 1
    OR qaqc_app_additions.manual_corrections_not_applied = 1;