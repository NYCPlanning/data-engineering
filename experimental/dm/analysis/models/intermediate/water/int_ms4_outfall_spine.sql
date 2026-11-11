SELECT
    * EXCLUDE (geom),
    {{ wastewater_facility_id('treatment_plant') }} AS wastewater_facility_id,
    geom
FROM {{ ref('stg_dep__ms4_outfalls') }}
