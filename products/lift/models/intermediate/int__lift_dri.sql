-- DRI is published at the NTA level. PLUTO doesn't carry NTA directly, so we
-- route bbl -> census tract (pluto.boroct2020) -> NTA (ct2020.nta2020) -> DRI.
-- Also the source of the census tract/NTA columns in lift_supplemented, since
-- they fall out of the same join path. The DRI join stays LEFT (not INNER) so a
-- missing DRI value (parks/cemeteries/airports - see README) doesn't also drop
-- the census tract/NTA for that bbl, which we still know regardless of DRI.
WITH lift AS (
    SELECT bbl FROM {{ ref('stg__lift_csv') }}
),

pluto AS (
    SELECT
        bbl,
        boroct2020
    FROM {{ ref('stg__pluto') }}
),

ct2020 AS (
    SELECT
        boroct2020,
        nta2020,
        ntaname
    FROM {{ ref('stg__ct2020') }}
),

dri AS (
    SELECT
        nta_code,
        dri_tier
    FROM {{ ref('stg__dri') }}
),

final AS (
    SELECT
        lift.bbl,
        ct2020.boroct2020,
        ct2020.nta2020,
        ct2020.ntaname,
        dri.dri_tier
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN ct2020 ON pluto.boroct2020 = ct2020.boroct2020
    LEFT JOIN dri ON ct2020.nta2020 = dri.nta_code
)

SELECT * FROM final
