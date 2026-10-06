-- Community district boundaries with each district's AHFT rank, for the map's AHFT layer.
-- AHFT ranks the 59 community districts. The other boundaries (parks, airports and other
-- joint interest areas) are unranked, so their ahft is null rather than false.
WITH districts AS (
    SELECT * FROM {{ ref('stg__dcp_cdboundaries') }}
),

ahft AS (
    SELECT borocd, ahft_rank FROM {{ ref('stg__dcp_housing_ahft') }}
)

SELECT
    districts.borocd,
    districts.community_district,
    ahft.ahft_rank,
    -- Same cutoff as lift_supplemented.ahft.
    CASE WHEN ahft.borocd IS NOT NULL THEN ahft.ahft_rank <= 12 END AS ahft,
    CASE
        WHEN ahft.borocd IS NULL THEN 'Not ranked'
        WHEN ahft.ahft_rank <= 12 THEN 'AHFT bottom 12'
        ELSE 'Other ranked district'
    END AS ahft_group,
    districts.geom
FROM districts
LEFT JOIN ahft ON districts.borocd = ahft.borocd
