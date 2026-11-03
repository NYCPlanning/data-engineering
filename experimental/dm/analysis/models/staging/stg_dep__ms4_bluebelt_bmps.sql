SELECT
    bmp_id,
    watershed,
    bmptype AS bmp_type,
    boro AS boro_code,
    ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'ms4_bluebelt_bmps') }}
