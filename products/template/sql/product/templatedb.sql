-- TODO determine the CD of each place
CREATE TABLE templatedb AS
(
    WITH
    libraries AS (SELECT * FROM libraries),

    green_spaces AS (SELECT * FROM green_spaces),

    historic_landmarks AS (SELECT * FROM historic_landmarks),

    all_records AS (
        SELECT
            library_name AS place_name,
            'library' AS place_type,
            borough,
            NULL AS bbl,
            wkb_geometry
        FROM
            libraries
        UNION ALL
        SELECT
            space_name AS place_name,
            'green space' AS place_type,
            borough,
            NULL AS bbl,
            wkb_geometry
        FROM green_spaces
        UNION ALL
        SELECT
            landmark_name AS place_name,
            'historic landmark' AS place_type,
            borough,
            bbl,
            wkb_geometry
        FROM historic_landmarks
    ),

    standardized_geometry_types AS (
        SELECT
            place_name,
            place_type,
            borough,
            bbl,
            CASE
                WHEN st_geometrytype(wkb_geometry) = 'ST_Polygon' THEN st_multi(wkb_geometry)
                WHEN st_isempty(wkb_geometry) THEN NULL
                ELSE wkb_geometry
            END AS wkb_geometry
        FROM all_records
    )

    SELECT * FROM standardized_geometry_types
    ORDER BY borough ASC, place_name ASC
);
