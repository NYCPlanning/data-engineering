SELECT
    sewer_area_type,
    ST_Union_Agg(geom) AS geom
FROM {{ ref('stg_dcp__separate_sewers_direct_drainage') }}
GROUP BY sewer_area_type
