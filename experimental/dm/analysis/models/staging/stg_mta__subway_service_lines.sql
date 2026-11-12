SELECT
    objectid AS service_line_id,
    service,
    service_name,
    geom
FROM {{ source('sources', 'mta_subway_service_lines') }}
