WITH tracts AS (
    SELECT * FROM {{ ref('int__tracts') }}
)

-- create 2 buffers around each tract: 1/4 mile and 1/2 mile
SELECT
    geoid,
    st_transform(geom, 2263) AS geom,
    st_buffer(st_transform(geom, 2263), 5280 * 0.25) AS quarter_mile_buffer_geom,
    st_buffer(st_transform(geom, 2263), 5280 * 0.5) AS half_mile_buffer_geom
FROM tracts
WHERE eligibility_flag
