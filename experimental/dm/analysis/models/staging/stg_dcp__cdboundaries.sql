SELECT
    borocd,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_cdboundaries') }}
