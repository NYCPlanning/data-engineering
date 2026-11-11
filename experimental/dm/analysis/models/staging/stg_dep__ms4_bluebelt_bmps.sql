SELECT
    "BMP_ID" AS bmp_id,
    "Watershed" AS watershed,
    "BMPType" AS bmp_type,
    "BORO" AS boro_code,
    ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'ms4_bluebelt_bmps') }}
