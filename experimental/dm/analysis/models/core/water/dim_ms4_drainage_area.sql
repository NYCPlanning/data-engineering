SELECT * FROM {{ ref('int_ms4_drainage_area_spine') }}
ORDER BY drains_to, waterbody, assumed_ownership
