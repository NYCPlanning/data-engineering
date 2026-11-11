SELECT
    spine.*,
    tcol.* EXCLUDE (boro_name)
FROM {{ ref('int_borough_spine') }} AS spine
LEFT JOIN {{ ref('int_borough_tcol_features') }} AS tcol ON spine.boro_name = tcol.boro_name
ORDER BY spine.boro_code
