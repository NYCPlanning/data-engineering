WITH cdboundaries_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cdboundaries') }}
),

final AS (
    SELECT
        borocd,
        -- Same format as AHFT's own district codes (e.g. 'BK10'), extended to the
        -- joint interest areas AHFT doesn't rank (e.g. 'MN64').
        CASE borocd // 100
            WHEN 1 THEN 'MN'
            WHEN 2 THEN 'BX'
            WHEN 3 THEN 'BK'
            WHEN 4 THEN 'QN'
            WHEN 5 THEN 'SI'
        END || LPAD((borocd % 100)::VARCHAR, 2, '0') AS community_district,
        wkb_geometry AS geom
    FROM cdboundaries_raw
)

SELECT * FROM final
