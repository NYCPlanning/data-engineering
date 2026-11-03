SELECT
    site_name,
    facilityty AS facility_type,
    agency,
    address,
    ms4,
    ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'ms4_waste_facilities') }}
