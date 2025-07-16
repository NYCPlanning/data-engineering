SELECT st_union(buffer_geom) AS buffer_geom
FROM {{ ref("int_spatial__shadow_nat_resources") }}
