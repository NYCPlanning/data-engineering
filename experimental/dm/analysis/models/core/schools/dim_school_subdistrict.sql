SELECT
    spine.* EXCLUDE (geom),
    utilization.* EXCLUDE (school_subdistrict_id),
    spine.geom
FROM {{ ref('int_school_subdistrict_spine') }} AS spine
LEFT JOIN {{ ref('int_school_subdistrict_utilization_features') }} AS utilization
    ON spine.school_subdistrict_id = utilization.school_subdistrict_id
ORDER BY spine.school_district, spine.subdistrict
