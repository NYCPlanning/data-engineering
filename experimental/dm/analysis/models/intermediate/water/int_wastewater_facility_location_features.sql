-- One point per facility: inside its site on DEP's MS4 map where it has one, which is DEP's own outline of the plant.
-- FacDB geocodes addresses and lots, and some land well off the plant, such as Wards Island's, which is just
-- WARDS ISLAND. The four plants without an MS4 site use FacDB's treatment plant points, named like JAMAICA WWTP.
WITH facdb AS (
    SELECT
        facility.wastewater_facility_id,
        plant.geom
    FROM {{ ref('int_wastewater_facility_spine') }} AS facility
    INNER JOIN {{ ref('stg_dcp__facilities') }} AS plant
        ON upper(facility.wastewater_facility_name) || ' WWTP' = plant.facility_name
    WHERE plant.facility_type = 'WASTEWATER TREATMENT PLANT' AND plant.geom IS NOT NULL
),

ms4_site AS (
    SELECT
        wastewater_facility_id,
        ST_PointOnSurface(ST_Union_Agg(geom)) AS geom
    FROM {{ ref('int_ms4_drainage_area_spine') }}
    WHERE drains_to_type = 'Wastewater facility'
    GROUP BY wastewater_facility_id
)

SELECT
    facility.wastewater_facility_id,
    coalesce(ms4_site.geom, facdb.geom) AS geom,
    CASE WHEN ms4_site.geom IS NOT NULL THEN 'MS4 map' WHEN facdb.geom IS NOT NULL THEN 'FacDB' END AS location_source
FROM {{ ref('int_wastewater_facility_spine') }} AS facility
LEFT JOIN facdb ON facility.wastewater_facility_id = facdb.wastewater_facility_id
LEFT JOIN ms4_site ON facility.wastewater_facility_id = ms4_site.wastewater_facility_id
