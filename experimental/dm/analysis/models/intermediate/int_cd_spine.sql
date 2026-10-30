-- CDTA codes map 1:1 to community districts: borough letters + the district number (BK01 -> 301).
-- CDTA names end in a note on how well the CDTA matches the district, e.g. "(CD 10 Approximation)".
{% set boundary_note = "' \\((CD|JIA) (\\d+) (Equivalent|Approximation)\\)$'" %}
WITH cdtas AS (
    SELECT
        *,
        borocode * 100 + right(cdta2020, 2)::integer AS borocd
    FROM {{ ref('stg_dcp__cdta2020') }}
)

SELECT
    districts.borocd,
    cdtas.borocode AS boro_code,
    cdtas.boro_name,
    districts.borocd % 100 AS cd_number,
    cdtas.boro_name
    || CASE WHEN cdtas.cdta_type = '1' THEN ' Joint Interest Area ' ELSE ' Community District ' END
    || (districts.borocd % 100)::varchar AS community_district_name,
    cdtas.cdta2020,
    regexp_replace(cdtas.cdta_name, {{ boundary_note }}, '') AS cdta_name,
    regexp_extract(cdtas.cdta_name, {{ boundary_note }}, 3) AS cdta_boundary_match,
    cdtas.cdta_type = '1' AS is_joint_interest_area,
    districts.geom
FROM {{ ref('stg_dcp__cdboundaries') }} AS districts
LEFT JOIN cdtas ON districts.borocd = cdtas.borocd
