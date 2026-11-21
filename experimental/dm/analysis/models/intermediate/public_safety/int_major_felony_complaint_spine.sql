-- ST_Within, as the old CD Profiles build used, so a point on a district line falls in no district rather than two.
-- Complaints in no district keep a null borocd so they still count citywide.
SELECT
    complaints.*,
    district.borocd
FROM {{ ref('stg_nypd__complaints') }} AS complaints
LEFT JOIN {{ ref('int_cd_spine') }} AS district
    ON ST_Within(complaints.geom, district.geom)
