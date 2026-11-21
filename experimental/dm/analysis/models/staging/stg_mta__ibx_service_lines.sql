SELECT
    feature_id AS ibx_track_id,
    feature_name AS track_name,
    geom
FROM {{ source('sources', 'mta_ibx_service_lines') }}
