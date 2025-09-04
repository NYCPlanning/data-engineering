CREATE TABLE green_spaces AS
(
    WITH
    parks_properties AS (SELECT * FROM stg_dpr_parksproperties),

    greenthumb_gardens AS (SELECT * FROM stg_dpr_greenthumb),

    all_green_spaces AS (
        SELECT
            space_name,
            borough,
            wkb_geometry
        FROM
            parks_properties
        UNION ALL
        SELECT
            space_name,
            borough,
            wkb_geometry
        FROM
            greenthumb_gardens
    ),

    standardized_values AS (
        SELECT
            space_name,
            CASE
                WHEN borough = 'B' THEN 'Brooklyn'
                WHEN borough = 'M' THEN 'Manhattan'
                WHEN borough = 'Q' THEN 'Queens'
                WHEN borough = 'R' THEN 'Staten Island'
                WHEN borough = 'X' THEN 'Bronx'
                ELSE borough
            END AS borough,
            wkb_geometry
        FROM
            all_green_spaces
    ),

    -- NOTE this causes all records in a borough named "Park" being merged
    merged_geometries AS (
        SELECT
            space_name,
            borough,
            ST_UNION(wkb_geometry) AS wkb_geometry
        FROM
            standardized_values
        GROUP BY space_name, borough
    )

    SELECT * FROM merged_geometries
);
