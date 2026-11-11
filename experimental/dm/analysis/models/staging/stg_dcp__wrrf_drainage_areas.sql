SELECT
    CASE "WPCP"
        WHEN '26W' THEN '26th_ward'
        WHEN 'BB' THEN 'bowery_bay'
        WHEN 'CI' THEN 'coney_island'
        WHEN 'HP' THEN 'hunts_point'
        WHEN 'JA' THEN 'jamaica'
        WHEN 'NC' THEN 'newtown_creek'
        WHEN 'NR' THEN 'north_river'
        WHEN 'OB' THEN 'oakwood_beach'
        WHEN 'OH' THEN 'owls_head'
        WHEN 'PR' THEN 'port_richmond'
        WHEN 'RH' THEN 'red_hook'
        WHEN 'RK' THEN 'rockaway'
        WHEN 'TI' THEN 'tallman_island'
        WHEN 'WI' THEN 'wards_island'
    END AS wastewater_facility_id,
    "WPCP" AS plant_code,
    "Drain_Area" AS sewershed_name,
    "Cap_MGD"::DOUBLE AS capacity_mgd,
    "CsewerAcre"::DOUBLE AS combined_sewer_acres,
    ST_Transform(ST_MakeValid(geom), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'dcp_wrrf_drainage_areas') }}
