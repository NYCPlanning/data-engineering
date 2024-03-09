SELECT
    zd as zoning_district,
    null AS change_type,
    geom,
    version
FROM 
    dcp_zoningdistricts_{{ envvar("first_version") }}
{% for versions in pairwise_versions -%}
    UNION ALL
    SELECT
        b.zd as zoning_district,
        NULL AS change_type, -- macro -> upzoning, downzoning, etc
        ST_INTERSECTION(a.geom, b.geom) AS geom,
        b.version
        FROM 
        dcp_zoningdistricts_{{ versions[0] }} AS a 
        INNER JOIN dcp_zoningdistricts_{{ versions[1] }} AS b
            ON ST_INTERSECTS(a.geom, b.geom) AND a.zd <> b.zd
{% endfor %}
