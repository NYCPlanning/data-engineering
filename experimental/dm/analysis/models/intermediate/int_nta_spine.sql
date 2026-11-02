SELECT * EXCLUDE (shape_area_sqft) FROM {{ ref('stg_dcp__nta2020') }}
