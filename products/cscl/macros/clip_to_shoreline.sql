{% macro clip_to_shoreline(geom_column='d.geom') %}
{#-
  Join clause pairing a district layer with the water overlapping each feature, for
  the "clipped to shoreline" half of the district boundary gdb. Use with
  `clipped_geom` in the select list, which does the actual subtraction.

  LEFT JOIN so features that touch no water survive with no water geometry, and
  LATERAL so the union covers only the pieces the feature actually intersects
  rather than the whole city.
-#}
LEFT JOIN LATERAL (
        SELECT st_union(m.geom) AS geom
        FROM {{ ref('int__water_mask') }} AS m
        WHERE st_intersects({{ geom_column }}, m.geom)
    ) AS water ON TRUE
{%- endmacro %}


{% macro clipped_geom(geom_column='d.geom', min_area=100) %}
{#-
  The district geometry with water removed, discarding parts below `min_area`.

  District edges and atomic polygon edges are nominally coincident but differ in the
  last bits, so subtracting water leaves hairline slivers along them — without the
  filter the difference produces ~109k sub-square-foot fragments across nymcea alone.
  Part areas fall into two clearly separated groups, nothing between 0.83 and 167
  square feet, and a 100 sq ft floor reproduces prod's row counts exactly on every
  affected layer (nycb2010/2020, nyct2010/2020, nyed, nypuma2020).

  Features left with no qualifying part come back empty rather than NULL, so callers
  can drop them with `WHERE NOT st_isempty(geom)`.
-#}
st_multi(coalesce(
        (
            SELECT st_union(part.geom)
            FROM (
                SELECT (st_dump(st_collectionextract(
                    st_difference(
                        {{ geom_column }},
                        coalesce(water.geom, st_setsrid('POLYGON EMPTY'::geometry, st_srid({{ geom_column }})))
                    ), 3
                ))).geom
            ) AS part
            WHERE st_area(part.geom) >= {{ min_area }}
        ),
        st_setsrid('MULTIPOLYGON EMPTY'::geometry, st_srid({{ geom_column }}))
    ))
{%- endmacro %}
