SELECT
    * EXCLUDE (geom),
    CASE
        WHEN drains_to = 'OVERLAND FLOW' THEN 'Overland flow'
        WHEN drains_to LIKE '% WWTP' THEN 'Wastewater facility'
        ELSE 'MS4 outfall'
    END AS drains_to_type,
    CASE WHEN drains_to LIKE '% WWTP' THEN {{ wastewater_facility_id('drains_to') }} END AS wastewater_facility_id,
    geom
FROM {{ ref('stg_dep__ms4_drainage_areas') }}
