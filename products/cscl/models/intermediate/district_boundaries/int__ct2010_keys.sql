{{ config(
    materialized = 'table'
) }}

-- One row per 2010 census tract (borough and tract): the BoroCT2010, NTACode and NTAName
-- nyct2010 and nyct2010wi write (CSCL-DISTRICTS-10). Prod copies BoroCT2010 from the
-- source tract's BOROCT and NTACode from its NEIGHBORHOOD_CODE, and looks NTAName up in
-- the first NeighborhoodPUMACodes row with that code (GDBExtractorClass.cs:960-1000,
-- 1597-1621). Ours keys the tract by its borough and tract, as the AtomicPolygons do, and
-- takes the NTA from the NeighborhoodPUMACodes row of that borough and tract. The two
-- differ where the source BOROCT isn't the borough and tract: on 26c the Brooklyn and
-- Staten Island pieces of water tract 990100, which the source keys 4990100 and QN99. The
-- default is prod's; key_tracts_by_borough on takes ours.

-- the row prod's lookup returns: the first NeighborhoodPUMACodes row with the code
WITH nta_by_code AS (
    SELECT DISTINCT ON (neighborhood_code)
        neighborhood_code,
        neighborhood_name
    FROM {{ source('recipe_sources', 'dcp_cscl_neighborhoodpumacodes') }}
    WHERE neighborhood_code IS NOT NULL
    ORDER BY neighborhood_code, ogc_fid
),

keys AS (
    SELECT
        ct.boroct,
        ct.source_boroct,
        ct.neighborhood_code AS nta_code_prod,
        nbc.neighborhood_name AS nta_name_prod,
        npc.neighborhood_code AS nta_code_borough,
        npc.neighborhood_name AS nta_name_borough
    FROM {{ ref('stg__censustract2010') }} AS ct
    LEFT JOIN nta_by_code AS nbc ON ct.neighborhood_code = nbc.neighborhood_code
    LEFT JOIN {{ ref('stg__neighborhoodpumacodes') }} AS npc
        ON ct.borocode = npc.borough AND ct.ct::int = npc.censustract
)

SELECT
    *,
    {% if var('key_tracts_by_borough') -%}
        boroct AS boroct2010,
        nta_code_borough AS nta_code,
        nta_name_borough AS nta_name
    {%- else -%}
        source_boroct AS boroct2010,
        nta_code_prod AS nta_code,
        nta_name_prod AS nta_name
    {%- endif %}
FROM keys
