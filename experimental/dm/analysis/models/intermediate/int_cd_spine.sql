-- CDTA codes map 1:1 to community districts: borough letters + the district number (BK01 -> 301).
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
    cdtas.cdta2020,
    cdtas.cdta_name,
    cdtas.cdta_type = '1' AS is_joint_interest_area,
    districts.geom
FROM {{ ref('stg_dcp__cdboundaries') }} AS districts
LEFT JOIN cdtas ON districts.borocd = cdtas.borocd
