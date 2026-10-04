{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2010 neighborhood tabulation areas. Boundaries come from the AtomicPolygon topology
-- (int__boundary__nta2010); columns, column order and types match the published layer.

-- borough holding most of the district's AP area - the borough it physically sits in
-- (Rikers Island's NTA QN0151 belongs to a Queens CDTA but is in the Bronx), robust to a
-- few APs across a borough line (QN99, Queens parks and cemeteries, has Brooklyn APs)
WITH district_borough AS (
    SELECT DISTINCT ON (m.nta2010)  -- noqa: AM01
        m.nta2010 AS entity_id,
        m.borocode
    FROM {{ ref('int__topology__ap_entities') }} AS m
    WHERE m.nta2010 IS NOT NULL
    GROUP BY m.nta2010, m.borocode
    ORDER BY m.nta2010, sum(m.area_sqft) DESC
),

boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__nta2010') }}
)

SELECT
    db.borocode::smallint AS "BoroCode",
    boro.boroname::varchar(13) AS "BoroName",
    boro.fips::varchar(3) AS "CountyFIPS",
    b.entity_id::varchar(4) AS "NTACode",
    npc.neighborhood_name::varchar(75) AS "NTAName",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN district_borough AS db ON b.entity_id = db.entity_id
LEFT JOIN {{ ref('stg__borough') }} AS boro ON db.borocode = boro.borocode
LEFT JOIN (
    SELECT DISTINCT
        neighborhood_code,
        neighborhood_name
    FROM {{ ref('stg__neighborhoodpumacodes') }}
) AS npc ON b.entity_id = npc.neighborhood_code
