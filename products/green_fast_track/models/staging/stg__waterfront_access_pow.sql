-- stg__waterfront_access_pow.sql

WITH source AS (
    SELECT *
    FROM {{ source('recipe_sources', 'dcp_waterfront_access_map_pow') }}
),

base AS (
    SELECT
        'waterfront_access_pow' AS variable_type,
        name,
        agency,
        name || '-' || agency AS variable_id,
        st_transform(wkb_geometry, 2263) AS raw_geom
    FROM source
),

filtered AS (
    SELECT
        variable_type,
        variable_id,
        name,
        agency,
        raw_geom
    FROM base
    WHERE
        upper(name) IN (
            'BUSH TERMINAL PIERS PARK', 'SHERMAN CREEK STREET END PARKS'
        )
        OR upper(agency) != 'NYC DPR'
)

SELECT
    variable_type,
    variable_id,
    st_union(raw_geom) AS raw_geom
FROM filtered
GROUP BY variable_type, variable_id
