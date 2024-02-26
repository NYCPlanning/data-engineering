-- TODO determine the CD of each place
CREATE TABLE templatedb AS
(
    WITH
    libraries AS (SELECT * FROM libraries),

    green_spaces AS (SELECT * FROM green_spaces),

    historic_landmarks AS (SELECT * FROM historic_landmarks),

    final AS (
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
            borough AS borough,
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
    )

    SELECT * FROM final
    ORDER BY borough ASC, place_name ASC
);
