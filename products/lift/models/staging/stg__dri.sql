WITH dri_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dri_subindices_indicators') }}
),

final AS (
    SELECT
        ntacode AS nta_code,
        ntaname AS nta_name,
        -- 5-level categorical: Lowest / Low / Intermediate / High / Highest
        displacementriskindex AS dri_tier
    FROM dri_raw
)

SELECT * FROM final
