WITH ct2020_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_ct2020') }}
),

final AS (
    SELECT
        boroct2020,
        nta2020,
        ntaname
    FROM ct2020_raw
)

SELECT * FROM final
