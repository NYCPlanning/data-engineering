SELECT
    transit_zone,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_transit_zones') }}
