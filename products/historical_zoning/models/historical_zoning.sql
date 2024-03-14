WITH intersects AS (
    SELECT
        zonedist as zoning_district,
        null AS change_type,
        wkb_geometry AS geom,
        data_library_version AS "version"
    FROM 
        main.dcp_zoningdistricts_{{ var("first_version") }}
    {% for versions in var("versions") -%}
        UNION ALL
        SELECT
            b.zonedist as zoning_district,
            NULL AS change_type, -- macro -> upzoning, downzoning, etc
            ST_INTERSECTION(a.wkb_geometry, b.wkb_geometry) AS geom,
            b.data_library_version AS "version"
            FROM 
            main.dcp_zoningdistricts_{{ versions[0] }} AS a 
            INNER JOIN main.dcp_zoningdistricts_{{ versions[1] }} AS b
                ON ST_INTERSECTS(a.wkb_geometry, b.wkb_geometry) AND a.zonedist <> b.zonedist
    {% endfor %}
),
dumped AS (
    SELECT 
        zoning_district, 
        change_type,
        (ST_DUMP(geom)).geom AS geom,
        "version"
    FROM intersects WHERE st_geometrytype(geom) = 'ST_Polygon' 
)
    SELECT 
        zoning_district, 
        change_type,
        geom,
        "version" 
    FROM intersects WHERE st_geometrytype(geom) = 'ST_Polygon' 
