{{ config(
    materialized='table',
    indexes=[
        {'columns': ['atomicid'], 'unique': True},
    ]
) }}

-- One row per in-scope AtomicPolygon, with one column per district type it belongs to.
-- This is the only place topology scope is decided and the only place membership
-- lives: the topology itself (vertices -> nodes -> edges) is keyed on atomicid and node
-- ids alone and is shared by every district type. Adding a district type means adding
-- a column here - by attribute (an AP column, or a lookup chain from one), never by
-- spatial join. NULL means the AP isn't assigned to any district of that type.
--
-- Scope (ap_topology_scope_pumas OR ap_topology_scope_boros; default citywide) must be
-- a union of whole districts for every district type compared against prod - a
-- district cut by the scope edge builds as a fragment.

{% set pumas = var('ap_topology_scope_pumas') %}
{% set boros = var('ap_topology_scope_boros') %}

SELECT DISTINCT
    a.atomicid,
    a.water_flag,
    -- for majority-by-area rollups (e.g. a district's borough) without geometry downstream
    st_area(a.geom) AS area_sqft,
    a.borocode,
    -- direct AP attributes, typed to match the published layers' keys
    a.borocode::int AS bb,
    a.assemdist::int AS ad,
    -- ED numbers repeat across assembly districts; AD * 1000 + ED is the published key
    a.assemdist::int * 1000 + a.electdist::int AS ed,
    nullif(a.schooldist, '')::int AS sd,
    -- school district as published in fgdb_nysd: District 10 once per borough (Bronx
    -- and Manhattan), every other district whole - including 9, 13, 14, 15 and 30, which
    -- also have a few APs on the far side of a borough line
    CASE
        WHEN a.schooldist = '10' THEN '10-' || a.borocode
        WHEN a.schooldist != '' THEN a.schooldist::int::text
    END AS sd_boro,
    nullif(a.commdist, '0')::int AS cd,
    a.borocode || a.censustract_2020 || a.censusblock_2020_raw AS cb2020,
    a.hurricane_evacuation_zone AS hez,
    -- via the AP's election district (THINED roster, keyed AD + ED): each ED belongs to
    -- exactly one council / congressional / state senate / municipal court district
    thined.city_council_district::int AS cc,
    thined.congress_district::int AS cg,
    thined.state_sen_district::int AS ss,
    -- municipal court numbers repeat per borough, so the id is borocode || court, as in
    -- fgdb_nymc; '00' is the unassigned placeholder, excluded from the published layer
    CASE WHEN thined.muni_court_district != '00' THEN thined.borough || thined.muni_court_district END AS mc,
    ct.boroct,
    -- same format as fgdb_nycb2010.bctcb2010: borocode(1) || ct2010(6) || cb2010(4)
    a.borocode || a.censustract_2010 || a.censusblock_2010_raw AS bctcb2010,
    ct.puma,
    -- via the AP's administering fire company and the THINFIRE company roster; NULL
    -- where admin_fire_company is '0' (unassigned islands/marshes, outside fgdb_nyfd)
    fc.fire_division,
    fc.fire_battalion,
    fc.unit_short AS fire_company,
    -- 2010 tract lookups
    ct.neighborhood_code AS nta2010,
    -- health areas follow 2010 tracts; codes repeat across boroughs, so the id is
    -- borocode || 4-digit code. nullif: water-only tracts (990100) carry ''
    a.borocode || lpad(nullif(ct.health_area, ''), 4, '0') AS health_area,
    ha.health_ct_district AS health_center,
    -- 2000 tract lookup
    ct00.mcea,
    -- 2020 hierarchy via the AP's 2020 tract: every tract has exactly one NTA and one
    -- CDTA, NTAs nest in CDTAs, and the union of each NTA's / CDTA's tracts matches
    -- the source NTA / CDTA polygons (verified 2026-10-09)
    ct20.boroct AS boroct2020,
    ct20.neighborhood_code AS nta2020,
    ct20.cdta_code AS cdta2020,
    ct20.puma AS puma2020
FROM {{ ref('stg__atomicpolygons') }} AS a
INNER JOIN {{ ref('stg__censustract2010') }} AS ct
    ON a.borocode = ct.borocode AND right(ct.boroct, 6) = a.censustract_2010
LEFT JOIN {{ ref('stg__censustract2020') }} AS ct20
    ON a.borocode = ct20.borocode AND a.censustract_2020 = ct20.ct
LEFT JOIN {{ ref('stg__censustract2000') }} AS ct00
    ON a.borocode = ct00.borocode AND a.censustract_2000 = ct00.ct
LEFT JOIN {{ ref('stg__healtharea') }} AS ha
    ON a.borocode = ha.borough AND ct.health_area = ha.healtharea
LEFT JOIN {{ ref('thined_by_field_unformatted') }} AS thined
    ON a.electdist = thined.election_district AND a.assemdist = thined.assembly_district
LEFT JOIN {{ ref('thinfire_by_field_unformatted') }} AS fc
    ON a.fire_company_type = fc.fire_company_type AND a.fire_company_number = fc.fire_company_number
WHERE
    FALSE
    {%- if pumas %}
    OR ct.puma IN ({{ "'" ~ (pumas | join("','")) ~ "'" }})
    {%- endif %}
    {%- if boros %}
        OR a.borocode IN ({{ "'" ~ (boros | join("','")) ~ "'" }})
    {%- endif %}
