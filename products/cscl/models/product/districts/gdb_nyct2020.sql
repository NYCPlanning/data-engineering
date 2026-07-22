{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- CDTANAME is upper-cased here but mixed-case in nynta2020; that inconsistency is
-- present in the published product and is preserved deliberately.

WITH clipped AS (
    SELECT
        d.ctlabel AS "CTLabel",
        d.borocode AS "BoroCode",
        b.boroname AS "BoroName",
        d.ct AS "CT2020",
        d.boroct AS "BoroCT2020",
        d.cd_eligibility AS "CDEligibil",
        nta.nta_name AS "NTAName",
        d.neighborhood_code AS "NTA2020",
        d.cdta_code AS "CDTA2020",
        cdta.cdta_name AS "CDTANAME",
        '36' || b.fips || d.ct AS "GEOID",
        d.puma AS puma,
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__censustract2020') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON d.borocode = b.borocode
    LEFT JOIN {{ ref('stg__ntaequiv2020') }} AS nta ON d.neighborhood_code = nta.nta_code
    LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta ON d.cdta_code = cdta.cdta_code
    {{ clip_to_shoreline('d.geom') }}
)

-- columns are listed rather than SELECT * so PUMA lands after the shape fields,
-- matching prod's field order
SELECT
    "CTLabel",
    "BoroCode",
    "BoroName",
    "CT2020",
    "BoroCT2020",
    "CDEligibil",
    "NTAName",
    "NTA2020",
    "CDTA2020",
    "CDTANAME",
    "GEOID",
    geom,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area",
    puma AS "PUMA"
FROM clipped
WHERE NOT st_isempty(geom)
