-- int_buffers__waterfront_access_pow.sql

WITH pow AS (
    SELECT * FROM {{ ref("stg__waterfront_access_pow") }}
),

filtered_name_agency AS (
    SELECT
        variable_type,
        name,
        agency,
        raw_geom
    FROM pow
    WHERE
        upper(name) IN (
            'BUSH TERMINAL PIERS PARK', 'SHERMAN CREEK STREET END PARKS'
        )
        OR upper(agency) != 'NYC DPR'
),

modified_id AS (
    SELECT
        variable_type,
        raw_geom,
        name || '-' || agency AS variable_id
    FROM filtered_name_agency
),

-- some areas are listed multiple times due to different geometries. Group them
grouped_by_variable_id AS (
    SELECT
        variable_type,
        variable_id,
        st_union(raw_geom) AS raw_geom
    FROM modified_id
    GROUP BY variable_type, variable_id
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    st_buffer(raw_geom, 200) AS buffer
FROM grouped_by_variable_id
