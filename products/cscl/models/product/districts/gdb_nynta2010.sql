{{ config(
    materialized='table'
) }}

-- 2010 neighborhood tabulation areas. Boundaries come from the AtomicPolygon topology
-- (int__boundary__nta2010); columns, column order and types match the published layer.
-- BoroCode and CountyFIPS come from int__nta2010_county_fips: CountyFIPS is prod's,
-- blank where the NTA's NeighborhoodPUMACodes FIPS code is, unless
-- fill_nta_county_fips_from_borough is on (CSCL-DISTRICTS-09).

WITH boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__nta2010') }}
)

SELECT
    cf.borocode::smallint AS "BoroCode",
    boro.boroname::varchar(13) AS "BoroName",
    cf.county_fips::varchar(3) AS "CountyFIPS",
    b.entity_id::varchar(4) AS "NTACode",
    npc.neighborhood_name::varchar(75) AS "NTAName",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN {{ ref('int__nta2010_county_fips') }} AS cf ON b.entity_id = cf.nta2010
LEFT JOIN {{ ref('stg__borough') }} AS boro ON cf.borocode = boro.borocode
LEFT JOIN (
    SELECT DISTINCT
        neighborhood_code,
        neighborhood_name
    FROM {{ ref('stg__neighborhoodpumacodes') }}
) AS npc ON b.entity_id = npc.neighborhood_code
