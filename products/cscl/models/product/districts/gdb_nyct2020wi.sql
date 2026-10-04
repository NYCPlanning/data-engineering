{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2020 census tracts, water included (the *wi variant). Boundaries come from the
-- AtomicPolygon topology (int__boundary__ct2020wi); columns, column order and types
-- match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__ct2020wi') }}
)

SELECT
    d.ctlabel::varchar(7) AS "CTLabel",
    d.borocode::varchar(1) AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    d.ct::varchar(6) AS "CT2020",
    b.entity_id::varchar(7) AS "BoroCT2020",
    d.cd_eligibility::varchar(1) AS "CDEligibil",
    nta.nta_name::varchar(75) AS "NTAName",
    d.neighborhood_code::varchar(6) AS "NTA2020",
    d.cdta_code::varchar(4) AS "CDTA2020",
    cdta.cdta_name::varchar(75) AS "CDTANAME",
    ('36' || boro.fips || d.ct)::varchar(11) AS "GEOID",
    d.puma::varchar(4) AS "PUMA",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__censustract2020') }} AS d ON b.entity_id = d.boroct
LEFT JOIN {{ ref('stg__borough') }} AS boro ON d.borocode = boro.borocode
LEFT JOIN {{ ref('stg__ntaequiv2020') }} AS nta ON d.neighborhood_code = nta.nta_code
LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta ON d.cdta_code = cdta.cdta_code
