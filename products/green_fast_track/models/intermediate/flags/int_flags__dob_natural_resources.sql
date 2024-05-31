-- this file should maybe be broken up, as it's doing two things
-- 1. supplying flags that go straight into int_flags__all
-- 2. supplying geoms that go into natural_resource shadows
-- 3. supplying geoms that go into natural_resource source outputs
WITH source_flags AS (
    SELECT * FROM {{ source('recipe_sources', 'dob_natural_resource_check_flags') }}
),

pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

joined AS (
    SELECT
        source_flags.*,
        pluto.geom AS raw_geom
    FROM source_flags
    INNER JOIN pluto
        ON (
            source_flags.boro_code || source_flags.block || lpad(source_flags.lot, 4, '0')
        ) = pluto.bbl
    WHERE pluto.geom IS NOT NULL
),

long_flags AS (
    SELECT
        bbl,
        'dob_coastal_erosion_hazard_area' AS variable_type,
        'DOB Coastal Erosion Hazard Area' AS variable_id,
        raw_geom
    FROM joined
    WHERE coastal_hazard_area_flag IS NOT NULL
    UNION ALL
    SELECT
        bbl,
        'dob_freshwater_wetland' AS variable_type,
        'DOB Freshwater Wetland' AS variable_id,
        raw_geom
    FROM joined
    WHERE fresh_water_wetlands_flag IS NOT NULL
    UNION ALL
    SELECT
        bbl,
        'dob_tidal_wetland' AS variable_type,
        'DOB Tidal Wetland' AS variable_id,
        raw_geom
    FROM joined
    WHERE tidal_coastal_wetlands_flag IS NOT NULL
)

SELECT
    bbl,
    'natural_resources' AS flag_id_field_name,
    variable_type,
    variable_id,
    raw_geom
FROM long_flags
