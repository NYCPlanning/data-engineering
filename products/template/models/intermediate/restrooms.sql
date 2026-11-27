{{ config(materialized='table') }}

WITH
restrooms AS (SELECT * FROM {{ ref('stg_moo_public_restrooms') }}),

boroughs AS (SELECT * FROM {{ ref('stg_dcp_boroboundaries_wi') }}),

operational AS (
    SELECT *
    FROM restrooms
    WHERE status = 'Operational'
),

with_borough AS (
    SELECT
        r.restroom_name,
        b.borough,
        r.location_type,
        r.operator,
        r.hours_of_operation,
        r.accessibility,
        r.wkb_geometry
    FROM operational AS r
    LEFT JOIN boroughs AS b
        ON ST_WITHIN(r.wkb_geometry, b.wkb_geometry)
)

SELECT * FROM with_borough
