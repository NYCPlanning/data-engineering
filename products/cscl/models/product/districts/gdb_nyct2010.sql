{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

WITH clipped AS (
    SELECT
        d.ctlabel AS "CTLabel",
        d.borocode AS "BoroCode",
        b.boroname AS "BoroName",
        d.ct AS "CT2010",
        d.boroct AS "BoroCT2010",
        d.cd_eligibility AS "CDEligibil",
        npc.neighborhood_code AS "NTACode",
        npc.neighborhood_name AS "NTAName",
        d.puma AS "PUMA",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__censustract2010') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON d.borocode = b.borocode
    LEFT JOIN {{ ref('stg__neighborhoodpumacodes') }} AS npc
        ON d.borocode = npc.borough AND d.ct::int = npc.censustract
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
