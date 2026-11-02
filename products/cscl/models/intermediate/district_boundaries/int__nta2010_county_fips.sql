{{ config(
    materialized = 'table'
) }}

-- One row per 2010 NTA: its borough and the CountyFIPS nynta2010 gets (CSCL-DISTRICTS-09).
-- Prod looks the FIPS code up in the NTA's NeighborhoodPUMACodes row (the first one its
-- query returns, GDBExtractorClass.cs:1597-1621) and pads it to three; where that FIPS code
-- is blank, or there is no row, it leaves CountyFIPS blank (and logs CSCL-LOG-GDB-05 or
-- -04). Ours takes the FIPS code of the NTA's borough, which maps to exactly one county. The
-- default is prod's; fill_nta_county_fips_from_borough on takes the borough's.

-- borough holding most of the district's AP area - the borough it physically sits in
-- (Rikers Island's NTA QN0151 belongs to a Queens CDTA but is in the Bronx), robust to a
-- few APs across a borough line (QN99, Queens parks and cemeteries, has Brooklyn APs)
WITH district_borough AS (
    SELECT DISTINCT ON (m.nta2010)  -- noqa: AM01
        m.nta2010,
        m.borocode
    FROM {{ ref('int__topology__ap_entities') }} AS m
    WHERE m.nta2010 IS NOT NULL
    GROUP BY m.nta2010, m.borocode
    ORDER BY m.nta2010, sum(m.area_sqft) DESC
),

-- the row prod's query returns: the NTA's first NeighborhoodPUMACodes row
npc AS (
    SELECT DISTINCT ON (neighborhood_code)
        neighborhood_code,
        nullif(trim(fips_code), '') AS fips_code
    FROM {{ source('recipe_sources', 'dcp_cscl_neighborhoodpumacodes') }}
    WHERE neighborhood_code IS NOT NULL
    ORDER BY neighborhood_code, ogc_fid
),

fips AS (
    SELECT
        db.nta2010,
        db.borocode,
        npc.fips_code AS npc_fips_code,
        lpad(npc.fips_code, 3, '0') AS county_fips_prod,
        boro.fips AS county_fips_borough
    FROM district_borough AS db
    LEFT JOIN npc ON db.nta2010 = npc.neighborhood_code
    LEFT JOIN {{ ref('stg__borough') }} AS boro ON db.borocode = boro.borocode
)

SELECT
    *,
    {% if var('fill_nta_county_fips_from_borough') -%}
        county_fips_borough
    {%- else -%}
        county_fips_prod
    {%- endif %} AS county_fips
FROM fips
