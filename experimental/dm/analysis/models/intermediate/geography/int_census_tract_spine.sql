SELECT * EXCLUDE (shape_area_sqft) FROM {{ ref('stg_dcp__ct2020') }}
