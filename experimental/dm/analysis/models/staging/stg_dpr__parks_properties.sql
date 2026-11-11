SELECT
    gispropnum AS park_id,
    signname AS park_name,
    typecategory AS park_type,
    acres::DOUBLE AS acres,
    ST_MakeValid(wkb_geometry) AS geom
FROM {{ source('sources', 'dpr_parksproperties') }}
