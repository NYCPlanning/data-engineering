DELETE FROM qaqc_historic WHERE version = :'VERSION';

INSERT INTO qaqc_historic (
    SELECT
        :'VERSION',
        sum(CASE WHEN b_likely_occ_desc != '0' THEN 1 ELSE 0 END) AS b_likely_occ_desc,
        sum(CASE WHEN b_large_alt_reduction != '0' THEN 1 ELSE 0 END) AS b_large_alt_reduction,
        sum(CASE WHEN b_nonres_with_units != '0' THEN 1 ELSE 0 END) AS b_nonres_with_units,
        sum(CASE WHEN units_co_prop_mismatch != '0' THEN 1 ELSE 0 END) AS units_co_prop_mismatch,
        sum(CASE WHEN partially_complete != '0' THEN 1 ELSE 0 END) AS partially_complete,
        sum(CASE WHEN units_init_null != '0' THEN 1 ELSE 0 END) AS units_init_null,
        sum(CASE WHEN units_prop_null != '0' THEN 1 ELSE 0 END) AS units_prop_null,
        sum(CASE WHEN units_res_accessory != '0' THEN 1 ELSE 0 END) AS units_res_accessory,
        sum(CASE WHEN outlier_demo_20plus != '0' THEN 1 ELSE 0 END) AS outlier_demo_20plus,
        sum(CASE WHEN outlier_nb_500plus != '0' THEN 1 ELSE 0 END) AS outlier_nb_500plus,
        sum(CASE WHEN outlier_top_alt_increase != '0' THEN 1 ELSE 0 END) AS outlier_top_alt_increase,
        sum(CASE WHEN dup_bbl_address_units != '0' THEN 1 ELSE 0 END) AS dup_bbl_address_units,
        sum(CASE WHEN dup_bbl_address != '0' THEN 1 ELSE 0 END) AS dup_bbl_address,
        sum(CASE WHEN inactive_with_update != '0' THEN 1 ELSE 0 END) AS inactive_with_update,
        sum(CASE WHEN no_work_job != '0' THEN 1 ELSE 0 END) AS no_work_job,
        sum(CASE WHEN geo_water != '0' THEN 1 ELSE 0 END) AS geo_water,
        sum(CASE WHEN geo_taxlot != '0' THEN 1 ELSE 0 END) AS geo_taxlot,
        sum(CASE WHEN geo_null_latlong != '0' THEN 1 ELSE 0 END) AS geo_null_latlong,
        sum(CASE WHEN geo_null_boundary != '0' THEN 1 ELSE 0 END) AS geo_null_boundary,
        sum(CASE WHEN invalid_date_filed != '0' THEN 1 ELSE 0 END) AS invalid_date_filed,
        sum(CASE WHEN invalid_date_lastupdt != '0' THEN 1 ELSE 0 END) AS invalid_date_lastupdt,
        sum(CASE WHEN invalid_date_statusd != '0' THEN 1 ELSE 0 END) AS invalid_date_statusd,
        sum(CASE WHEN invalid_date_statusp != '0' THEN 1 ELSE 0 END) AS invalid_date_statusp,
        sum(CASE WHEN invalid_date_statusr != '0' THEN 1 ELSE 0 END) AS invalid_date_statusr,
        sum(CASE WHEN invalid_date_statusx != '0' THEN 1 ELSE 0 END) AS invalid_date_statusx,
        sum(CASE WHEN incomp_tract_home != '0' THEN 1 ELSE 0 END) AS incomp_tract_home,
        sum(CASE WHEN dem_nb_overlap != '0' THEN 1 ELSE 0 END) AS dem_nb_overlap
    FROM final_qaqc
);
