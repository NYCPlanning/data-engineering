CREATE TABLE libraries AS
(
    WITH
    brooklyn_libraries AS (SELECT * FROM stg_bpl_libraries),

    queens_libraries AS (SELECT * FROM stg_qpl_libraries),

    all_other_libraries AS (SELECT * FROM stg_nypl_libraries),

    all_other_libraries_standardized AS (
        SELECT
            library_name,
            wkb_geometry,
            CASE
                WHEN borough = 'New York' THEN 'Manhattan'
                ELSE borough
            END AS borough
        FROM
            all_other_libraries
    ),

    final AS (
        SELECT
            library_name,
            borough,
            wkb_geometry
        FROM
            all_other_libraries_standardized
        UNION ALL
        SELECT
            library_name,
            'Brooklyn' AS borough,
            wkb_geometry
        FROM brooklyn_libraries
        UNION ALL
        SELECT
            library_name,
            'Queens' AS borough,
            wkb_geometry
        FROM queens_libraries
    )

    SELECT * FROM final
);
