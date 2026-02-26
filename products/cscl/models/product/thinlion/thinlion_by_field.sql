WITH atomic_polygons_with_lookups AS (
    SELECT
        ap.borocode AS borough,
        ap.censustract_2020 AS census_tract_2020,
        RIGHT(ap.atomicid, 3) AS dynamic_block,
        ap.censusblock_2020_basic AS census_block_2020,
        ap.censusblock_2020_suffix AS census_block_suffix_2020,
        ap.censustract_1990 AS census_tract_1990,
        ct2010.cd_eligibility AS community_development_eligibility,
        ap.commdist AS community_district,
        ct2010.mcea AS minor_census_economic_area,
        ct2010.health_area,
        ha.health_ct_district AS health_center_district,
        NULL AS police_patrol_borough_command,  -- TL12: NYPDPrecinct doesn't have this field
        prec.precinct AS police_precinct,
        ap.water_flag AS water_block_mapping_suppression_flag,
        ap.fire_company_type,
        ap.fire_company_number,
        ap.borocode AS sanborn_borough_1,
        ap.sb1_volume AS sanborn_volume_1,
        ap.sb1_page AS sanborn_page_1,
        ap.borocode AS sanborn_borough_2,
        ap.sb2_volume AS sanborn_volume_2,
        ap.sb2_page AS sanborn_page_2,
        ap.borocode AS sanborn_borough_3,
        ap.sb3_volume AS sanborn_volume_3,
        ap.sb3_page AS sanborn_page_3,
        ap.censustract_2000 AS census_tract_2000,
        ap.censusblock_2000_basic AS census_block_2000,
        ap.censusblock_2000_suffix AS census_block_suffix_2000,
        ap.assemdist AS assembly_district,
        ap.electdist AS election_district,
        ap.hurricane_evacuation_zone,
        CASE 
            WHEN pb.patrol_borough = 'Manhattan South' THEN '1'
            WHEN pb.patrol_borough = 'Manhattan North' THEN '2'
            WHEN pb.patrol_borough = 'Bronx' THEN '3'
            WHEN pb.patrol_borough = 'Brooklyn South' THEN '4'
            WHEN pb.patrol_borough = 'Brooklyn North' THEN '5'
            WHEN pb.patrol_borough = 'Queens North' THEN '6'
            WHEN pb.patrol_borough = 'Staten Island' THEN '7'
            WHEN pb.patrol_borough = 'Queens South' THEN '8'
        END AS patrol_borough,
        beat.sector AS police_sector,
        ap.censustract_2010_basic AS census_tract_2010_basic,
        ap.censustract_2010_suffix AS census_tract_2010_suffix,
        ap.censusblock_2010_basic AS census_block_2010,
        ap.censusblock_2010_suffix AS census_block_suffix_2010,
        ct2020.neighborhood_code AS nta2020,
        ct2020.cdta_code AS cdta,
        ap.commercial_waste_zone AS cwz,
        ct2020.puma AS puma2020
    FROM {{ ref("stg__atomicpolygons") }} ap
    -- Join CensusTract2010 via concatenated key
    LEFT JOIN {{ ref("stg__censustract2010") }} ct2010
        ON ap.borocode || ap.censustract_2010 = ct2010.boroct
    -- Join CensusTract2020 via concatenated key
    LEFT JOIN {{ ref("stg__censustract2020") }} ct2020
        ON ap.borocode || ap.censustract_2020 = ct2020.boroct
    -- Join HealthArea via health_area from CensusTract2010
    LEFT JOIN {{ ref("stg__healtharea") }} ha
        ON ct2010.health_area = ha.healtharea
    -- Spatial joins using centroid point-in-polygon
    LEFT JOIN {{ ref("stg__nypdprecinct") }} prec
        ON ST_Within(ST_Centroid(ap.geom), prec.geom)
    LEFT JOIN {{ ref("stg__nypdpatrolborough") }} pb
        ON ST_Within(ST_Centroid(ap.geom), pb.geom)
    LEFT JOIN {{ ref("stg__nypdbeat") }} beat
        ON ST_Within(ST_Centroid(ap.geom), beat.geom)
)

SELECT
    {{ apply_text_formatting_from_seed('text_formatting__thinlion_dat') }}
FROM atomic_polygons_with_lookups
ORDER BY census_tract_2020, dynamic_block

