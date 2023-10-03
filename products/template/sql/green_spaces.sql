-- TODO determine the Borough of each green space
CREATE TABLE green_spaces AS
(
    WITH
    parks_properties AS (SELECT * FROM stg_dpr_parksproperties),

    greenthumb_gardens AS (SELECT * FROM stg_dpr_greenthumb),

    final AS (
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
    )

    SELECT * FROM final
);
