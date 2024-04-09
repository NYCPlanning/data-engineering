CREATE TABLE templatedb_boroughs AS
(
    WITH
    templatedb AS (SELECT * FROM templatedb),

    final AS (
        SELECT
            borough,
            count(place_name) AS places_count,
            sum(CASE WHEN place_type = 'library' THEN 1 ELSE 0 END)
            AS library_count,
            sum(CASE WHEN place_type = 'green space' THEN 1 ELSE 0 END)
            AS green_space_count,
            sum(CASE WHEN place_type = 'historic landmark' THEN 1 ELSE 0 END)
            AS historic_landmark_count,
            sum(CASE WHEN bbl IS NULL THEN 1 ELSE 0 END)
            AS places_without_bbl,
            sum(CASE WHEN wkb_geometry IS NULL THEN 1 ELSE 0 END)
            AS places_without_geometry
        FROM templatedb
        GROUP BY borough
    )

    SELECT * FROM final
    ORDER BY places_count DESC
);
