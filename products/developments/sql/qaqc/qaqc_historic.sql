DELETE FROM qaqc_historic WHERE version = :'VERSION';

INSERT INTO qaqc_historic (
    SELECT
        :'VERSION' AS version,
        SUM(CASE WHEN b_likely_occ_desc != '0' THEN 1 ELSE 0 END) AS b_likely_occ_desc,
        SUM(CASE WHEN b_large_alt_reduction != '0' THEN 1 ELSE 0 END) AS b_large_alt_reduction,
        SUM(CASE WHEN b_nonres_with_units != '0' THEN 1 ELSE 0 END) AS b_nonres_with_units,
        SUM(CASE WHEN units_co_prop_mismatch != '0' THEN 1 ELSE 0 END) AS units_co_prop_mismatch,
        SUM(CASE WHEN partially_complete != '0' THEN 1 ELSE 0 END) AS partially_complete,
        SUM(CASE WHEN units_init_null != '0' THEN 1 ELSE 0 END) AS units_init_null,
        SUM(CASE WHEN units_prop_null != '0' THEN 1 ELSE 0 END) AS units_prop_null,
        SUM(CASE WHEN units_res_accessory != '0' THEN 1 ELSE 0 END) AS units_res_accessory,
        SUM(CASE WHEN outlier_demo_20plus != '0' THEN 1 ELSE 0 END) AS outlier_demo_20plus,
        SUM(CASE WHEN outlier_nb_500plus != '0' THEN 1 ELSE 0 END) AS outlier_nb_500plus,
        SUM(CASE WHEN outlier_top_alt_increase != '0' THEN 1 ELSE 0 END) AS outlier_top_alt_increase,
        SUM(CASE WHEN dup_bbl_address_units != '0' THEN 1 ELSE 0 END) AS dup_bbl_address_units,
        SUM(CASE WHEN dup_bbl_address != '0' THEN 1 ELSE 0 END) AS dup_bbl_address,
        SUM(CASE WHEN inactive_with_update != '0' THEN 1 ELSE 0 END) AS inactive_with_update,
        SUM(CASE WHEN no_work_job != '0' THEN 1 ELSE 0 END) AS no_work_job,
        SUM(CASE WHEN geo_water != '0' THEN 1 ELSE 0 END) AS geo_water,
        SUM(CASE WHEN geo_taxlot != '0' THEN 1 ELSE 0 END) AS geo_taxlot,
        SUM(CASE WHEN geo_null_latlong != '0' THEN 1 ELSE 0 END) AS geo_null_latlong,
        SUM(CASE WHEN geo_null_boundary != '0' THEN 1 ELSE 0 END) AS geo_null_boundary,
        SUM(CASE WHEN invalid_date_filed != '0' THEN 1 ELSE 0 END) AS invalid_date_filed,
        SUM(CASE WHEN invalid_date_lastupdt != '0' THEN 1 ELSE 0 END) AS invalid_date_lastupdt,
        SUM(CASE WHEN invalid_date_statusd != '0' THEN 1 ELSE 0 END) AS invalid_date_statusd,
        SUM(CASE WHEN invalid_date_statusp != '0' THEN 1 ELSE 0 END) AS invalid_date_statusp,
        SUM(CASE WHEN invalid_date_statusr != '0' THEN 1 ELSE 0 END) AS invalid_date_statusr,
        SUM(CASE WHEN invalid_date_statusx != '0' THEN 1 ELSE 0 END) AS invalid_date_statusx,
        SUM(CASE WHEN incomp_tract_home != '0' THEN 1 ELSE 0 END) AS incomp_tract_home,
        SUM(CASE WHEN dem_nb_overlap != '0' THEN 1 ELSE 0 END) AS dem_nb_overlap
    FROM final_qaqc
);
