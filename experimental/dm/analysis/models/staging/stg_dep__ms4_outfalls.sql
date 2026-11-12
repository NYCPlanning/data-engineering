SELECT
    id AS outfall_id,
    waterbody AS waterbody,
    floatables = 'YES' AS floatables_of_concern,
    pathogens = 'YES' AS pathogens_of_concern,
    nitrogen = 'YES' AS nitrogen_of_concern,
    phosphorus = 'YES' AS phosphorus_of_concern,
    boro AS boro_name,
    treatment_plant AS treatment_plant,
    outfalltype AS outfall_type,
    ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'ms4_outfalls') }}
