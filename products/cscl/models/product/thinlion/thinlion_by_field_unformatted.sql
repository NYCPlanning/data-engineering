WITH atomic_polygons_with_lookups AS (
    SELECT
        ap.atomicid,
        ap.borocode AS borough,
        ap.dynamic_block AS dynamic_block,
        {% for year in ['2020', '2010', '2000'] %}
        -- census {{ year }}
        ap.censustract_{{ year }},
        ap.censustract_{{ year }}_basic,
        ap.censustract_{{ year }}_suffix,
        ap.censusblock_{{ year }}_basic,
        ap.censusblock_{{ year }}_suffix,
        {% endfor %}
        -- 1990 is a little weird
        ap.censustract_1990_basic,
        CASE
            WHEN ap.censustract_1990_suffix IS NULL OR ap.censustract_1990_suffix = 0 THEN ''
            ELSE LPAD(ap.censustract_1990_suffix::TEXT, 2, '0')
        END AS censustract_1990_suffix,

        ct2010.cd_eligibility AS community_development_eligibility,
        ap.commdist AS community_district,
        ct2010.mcea AS minor_census_economic_area,
        ct2010.health_area,
        ha.health_ct_district AS health_center_district,
        -- NULL AS police_patrol_borough_command,  -- TL12: NYPDPrecinct doesn't have this field
        prec.precinct AS police_precinct,
        ap.water_flag AS water_block_mapping_suppression_flag,
        CASE
            WHEN TRIM(ap.fire_company_type) IN ('', 'null', 'NULL', '0') THEN ''
            ELSE LEFT(ap.fire_company_type, 1)
        END as fire_company_type,
        -- ap.fire_company_number,
        CASE
            WHEN TRIM(COALESCE(ap.fire_company_type, '')) IN ('', '0', 'null') THEN ''
            WHEN TRIM(COALESCE(ap.fire_company_number, '')) IN ('', '0', 'null') THEN ''
            ELSE TRIM(fire_company_number)
        END AS fire_company_number,
        -- sanborn 1: if any field is empty, all fields should be empty
        CASE 
            WHEN TRIM(COALESCE(ap.sb1_volume, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb1_page, '')) = '' THEN ''
            ELSE ap.borocode
        END AS sanborn_borough_1,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb1_page, '')) = '' THEN ''
            ELSE ap.sb1_volume
        END AS sanborn_volume_1,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb1_volume, '')) = '' THEN ''
            ELSE ap.sb1_page
        END AS sanborn_page_1,
        -- sanborn 2: if any field is empty, all fields should be empty
        CASE 
            WHEN TRIM(COALESCE(ap.sb2_volume, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb2_page, '')) = '' THEN ''
            ELSE ap.borocode
        END AS sanborn_borough_2,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb2_page, '')) = '' THEN ''
            ELSE ap.sb2_volume
        END AS sanborn_volume_2,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb2_volume, '')) = '' THEN ''
            ELSE ap.sb2_page
        END AS sanborn_page_2,
        -- sanborn 3: if any field is empty, all fields should be empty
        CASE 
            WHEN TRIM(COALESCE(ap.sb3_volume, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb3_page, '')) = '' THEN ''
            ELSE ap.borocode
        END AS sanborn_borough_3,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb3_page, '')) = '' THEN ''
            ELSE ap.sb3_volume
        END AS sanborn_volume_3,
        CASE 
            WHEN TRIM(COALESCE(ap.borocode, '')) = '' THEN ''
            WHEN TRIM(COALESCE(ap.sb3_volume, '')) = '' THEN ''
            ELSE ap.sb3_page
        END AS sanborn_page_3,
        ap.assemdist AS assembly_district,
        ap.electdist AS election_district,
        CASE
            WHEN ap.hurricane_evacuation_zone = '7' THEN '0'
            ELSE ap.hurricane_evacuation_zone
        END as hurricane_evacuation_zone,
        CASE
            WHEN pb.patrol_borough = 'MS' THEN '1'
            WHEN pb.patrol_borough = 'MN' THEN '2'
            WHEN pb.patrol_borough = 'BX' THEN '3'
            WHEN pb.patrol_borough = 'BS' THEN '4'
            WHEN pb.patrol_borough = 'BN' THEN '5'
            WHEN pb.patrol_borough = 'QN' THEN '6'
            WHEN pb.patrol_borough = 'SI' THEN '7'
            WHEN pb.patrol_borough = 'QS' THEN '8'
        END AS police_patrol_borough_command,
        pb.patrol_borough AS patrol_borough,
        beat.sector AS police_sector,
        ct2020.neighborhood_code AS nta2020,
        ct2020.cdta_code AS cdta,
        ap.commercial_waste_zone AS cwz,
        ct2020.puma AS puma2020,
        -- Foreign key IDs for tracing
        ct2010.globalid AS ct2010_globalid,
        ct2020.globalid AS ct2020_globalid,
        ha.globalid AS healtharea_globalid,
        prec.globalid AS nypdprecinct_globalid,
        pb.globalid AS nypdpatrolborough_globalid,
        beat.globalid AS nypdbeat_globalid
    FROM {{ ref("stg__atomicpolygons") }} ap
    -- Join CensusTract2010 via concatenated key
    LEFT JOIN {{ ref("stg__censustract2010") }} ct2010
        ON ap.borocode || ap.censustract_2010 = ct2010.boroct
    -- Join CensusTract2020 via concatenated key
    LEFT JOIN {{ ref("stg__censustract2020") }} ct2020
        ON ap.borocode || ap.censustract_2020 = ct2020.boroct
    -- Join HealthArea via health_area from CensusTract2010
    LEFT JOIN {{ ref("stg__healtharea") }} ha
        ON ct2010.health_area = ha.healtharea AND ct2010.borocode = ha.borough
    -- Spatial joins using centroid point-in-polygon
    LEFT JOIN {{ ref("stg__nypdprecinct") }} prec
        ON ST_Within(ST_Centroid(ap.geom), prec.geom)
    LEFT JOIN {{ ref("stg__nypdpatrolborough") }} pb
        ON ST_Within(ST_Centroid(ap.geom), pb.geom)
    LEFT JOIN {{ ref("stg__nypdbeat") }} beat
        ON ST_Within(ST_Centroid(ap.geom), beat.geom)
)

SELECT * FROM atomic_polygons_with_lookups
ORDER BY borough, censustract_2020_basic, censustract_2020_suffix, dynamic_block
