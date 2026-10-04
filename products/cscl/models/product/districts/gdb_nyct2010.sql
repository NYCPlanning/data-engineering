{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2010 census tracts. Boundaries come from the AtomicPolygon topology
-- (int__boundary__ct2010); columns, column order and types match the published layer.

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__ct2010') }}
)

SELECT
    d.ctlabel::varchar(7) AS "CTLabel",
    d.borocode::varchar(1) AS "BoroCode",
    boro.boroname::varchar(32) AS "BoroName",
    d.ct::varchar(6) AS "CT2010",
    b.entity_id::varchar(7) AS "BoroCT2010",
    d.cd_eligibility::varchar(1) AS "CDEligibil",
    npc.neighborhood_code::varchar(4) AS "NTACode",
    npc.neighborhood_name::varchar(75) AS "NTAName",
    d.puma::varchar(4) AS "PUMA",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('stg__censustract2010') }} AS d ON b.entity_id = d.boroct
LEFT JOIN {{ ref('stg__borough') }} AS boro ON d.borocode = boro.borocode
LEFT JOIN {{ ref('stg__neighborhoodpumacodes') }} AS npc
    ON d.borocode = npc.borough AND d.ct::int = npc.censustract
