{{ config(materialized='table') }}

-- TODO determine the CD of each place
WITH
libraries AS (
    SELECT
        libraries.*,
        nearest_park.nearest_park,
        nearest_park.nearest_park_distance_ft
    FROM {{ ref('libraries') }} AS libraries
    LEFT JOIN {{ ref('libraries_nearest_park') }} AS nearest_park
        ON
            libraries.library_name = nearest_park.library_name
            AND libraries.borough = nearest_park.borough
),

green_spaces AS (SELECT * FROM {{ ref('green_spaces') }}),

historic_landmarks AS (SELECT * FROM {{ ref('historic_landmarks') }}),

all_records AS (
    SELECT
        library_name AS place_name,
        'library' AS place_type,
        borough,
        NULL AS bbl,
        address,
        nearest_park,
        nearest_park_distance_ft,
        wkb_geometry
    FROM libraries
    UNION ALL
    SELECT
        space_name AS place_name,
        'green space' AS place_type,
        borough,
        NULL AS bbl,
        NULL AS address,
        NULL AS nearest_park,
        NULL AS nearest_park_distance_ft,
        wkb_geometry
    FROM green_spaces
    UNION ALL
    SELECT
        landmark_name AS place_name,
        'historic landmark' AS place_type,
        borough,
        bbl,
        NULL AS address,
        NULL AS nearest_park,
        NULL AS nearest_park_distance_ft,
        wkb_geometry
    FROM historic_landmarks
),

standardized_geometry_types AS (
    SELECT
        place_name,
        place_type,
        borough,
        bbl,
        address,
        nearest_park,
        nearest_park_distance_ft,
        CASE
            WHEN ST_GEOMETRYTYPE(wkb_geometry) = 'ST_Polygon' THEN ST_MULTI(wkb_geometry)
            WHEN ST_ISEMPTY(wkb_geometry) THEN NULL
            ELSE wkb_geometry
        END AS wkb_geometry
    FROM all_records
)

SELECT * FROM standardized_geometry_types
ORDER BY borough ASC, place_name ASC
