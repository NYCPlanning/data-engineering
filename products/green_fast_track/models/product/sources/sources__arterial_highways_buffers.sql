SELECT ST_UNION(buffer) AS geom
FROM {{ ref('stg__dcm_arterial_highways') }}
