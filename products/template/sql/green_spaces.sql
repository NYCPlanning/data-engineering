-- TODO determine the Borough of each green space
CREATE TABLE green_spaces AS
(
    WITH
    parks_properties AS (SELECT * FROM stg_dpr_parksproperties),

    greenthumb_gardens AS (SELECT * FROM stg_dpr_greenthumb),

    all_green_spaces AS (
        SELECT
            space_name,
            wkb_geometry
        FROM
            parks_properties
        UNION ALL
        SELECT
            space_name,
            wkb_geometry
        FROM
            greenthumb_gardens
    ),

    -- NOTE this causes all records in a borough named "Park" being merged
    merged_geometries AS (
        SELECT
            space_name,
            ST_UNION(wkb_geometry) AS wkb_geometry
        FROM
            all_green_spaces
        GROUP BY space_name
    )

    SELECT * FROM merged_geometries
);
