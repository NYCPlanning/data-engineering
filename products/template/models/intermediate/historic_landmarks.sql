{{ config(materialized='table') }}

WITH
landmarks AS (SELECT * FROM {{ ref('stg_lpc_landmarks') }}),

boroughs AS (SELECT * FROM {{ ref('boroughs') }}),

landmarks_reprojected AS (
    SELECT
        landmark_name,
        borough_name_short,
        bbl,
        ST_TRANSFORM(ST_SETSRID(wkb_geometry, 2263), 4326) AS wkb_geometry
    FROM
        landmarks
),

landmarks_borough_names AS (
    SELECT
        landmarks_reprojected.*,
        boroughs.name AS borough
    FROM landmarks_reprojected LEFT JOIN
        boroughs ON landmarks_reprojected.borough_name_short = boroughs.name_short
),

merged_geometries AS (
    SELECT
        landmark_name,
        borough,
        bbl,
        ST_UNION(wkb_geometry) AS wkb_geometry
    FROM
        landmarks_borough_names
    GROUP BY landmark_name, borough, bbl
)

SELECT * FROM merged_geometries
