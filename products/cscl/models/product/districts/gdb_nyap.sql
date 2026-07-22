{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area",
    d.borocode AS "BOROUGH",
    d.censusblock_2000_basic::text AS "CENSUSBLOCK_2000",
    d.censusblock_2000_suffix AS "CENSUSBLOCK_2000_SUFFIX",
    d.censustract_2000 AS "CENSUSTRACT_2000",
    d.censusblock_2010_raw AS "CENSUSBLOCK_2010",
    d.censusblock_2010_suffix::text AS "CENSUSBLOCK_2010_SUFFIX",
    d.censustract_2010 AS "CENSUSTRACT_2010",
    d.censustract_1990 AS "CENSUSTRACT_1990",
    d.admin_fire_company AS "ADMIN_FIRE_COMPANY",
    d.water_flag AS "WATER_FLAG",
    d.assemdist AS "ASSEMDIST",
    d.electdist AS "ELECTDIST",
    d.schooldist AS "SCHOOLDIST",
    d.commdist AS "COMMDIST",
    d.sb1_volume AS "SB1_VOLUME",
    d.sb1_page AS "SB1_PAGE",
    d.sb2_volume AS "SB2_VOLUME",
    d.sb2_page AS "SB2_PAGE",
    d.sb3_volume AS "SB3_VOLUME",
    d.sb3_page AS "SB3_PAGE",
    d.atomicid AS "ATOMICID",
    d.atomic_num AS "ATOMIC_NUM",
    d.hurricane_evacuation_zone AS "HURRICANE_EVACUATION_ZONE",
    d.censustract_2020 AS "CENSUSTRACT_2020",
    d.censusblock_2020_basic::text AS "CENSUSBLOCK_2020",
    d.censusblock_2020_suffix::text AS "CENSUSBLOCK_2020_SUFFIX",
    d.commercial_waste_zone AS "COMMERCIAL_WASTE_ZONE",
    d.geom
FROM {{ ref('stg__atomicpolygons') }} AS d
