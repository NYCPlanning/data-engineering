-- cpdb_projects_shp with each project's borough and community district, for files shared
-- outside the published CPDB outputs. Column names stay within the shapefile's 10 characters.
SELECT
    {{ dbt_utils.star(from=ref('cpdb_projects_shp'), relation_alias='s', except=['geom']) }},
    -- the CSV has no geometry, so this says which projects are mapped
    CASE ST_GEOMETRYTYPE(s.geom)
        WHEN 'ST_MultiPoint' THEN 'points'
        WHEN 'ST_MultiPolygon' THEN 'polygons'
    END AS mapped_as,
    b.borough,
    b.borough = 'multiple' AS multi_boro,
    b.boroughs AS boros,
    b.borough_source AS boro_src,
    b.community_district AS cd,
    b.community_district = 'multiple' AS multi_cd,
    b.community_districts AS cds,
    b.community_district_source AS cd_src,
    b.dashboard_citywide AS citywide,
    s.geom
FROM {{ ref('cpdb_projects_shp') }} AS s
LEFT JOIN {{ ref('int__project_boundaries') }} AS b ON s.maprojid = b.maprojid
