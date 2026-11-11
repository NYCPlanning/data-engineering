-- One row has no geometry, and a few, including the direct drainage polygon, aren't valid.
SELECT
    CASE "COMB_OR_SE"
        WHEN 'SEPARATE' THEN 'Separate'
        WHEN 'DIRECT DRAINAGE' THEN 'Direct drainage'
    END AS sewer_area_type,
    "WPCP" AS plant_code,
    "DATE_REVIS" AS date_revised,
    ST_Transform(ST_MakeValid(geom), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'dcp_separate_sewers_direct_drainage') }}
WHERE geom IS NOT NULL
