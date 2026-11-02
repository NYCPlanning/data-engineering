SELECT
    borocd,
    shape_area AS shape_area_sqft,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_cdboundaries') }}
