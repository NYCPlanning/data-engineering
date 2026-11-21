-- Xcoord and Ycoord repeat the point's EPSG:2263 coordinates, so they're left out.
SELECT
    name,
    permitno AS permit_number,
    permittype AS permit_type,
    address,
    zip::integer AS zip_code,
    borough AS boro_name,
    bbl::bigint AS bbl,
    ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'ms4_spdes_permits') }}
