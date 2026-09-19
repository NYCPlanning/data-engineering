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
  The district geometry with water removed, discarding parts below `min_area` - and,
  within each surviving part, discarding interior rings (holes) below the same floor.

  District edges and atomic polygon edges are nominally coincident but differ in the
  last bits, so subtracting water leaves hairline slivers along them — without the
  filter the difference produces ~109k sub-square-foot fragments across nymcea alone.
  Part areas fall into two clearly separated groups, nothing between 0.83 and 167
  square feet, and a 100 sq ft floor reproduces prod's row counts exactly on every
  affected layer (nycb2010/2020, nyct2010/2020, nyed, nypuma2020).

  The same slivering shows up *within* a single surviving part as sub-square-foot
  interior rings, not just as separate top-level parts - e.g. PUMA 4105 (nypuma2010)
  differences to one real part plus 4,926 "holes", every single one under 0.24 sq ft
  (see CSCL-DISTRICTS-01, docs/prod_bugs). Untouched, GDAL's OpenFileGDB writer/reader
  round-trip can't unambiguously reconstruct a polygon with that many rings (its
  organizePolygons() heuristic, meant for formats with no explicit ring/hole
  structure, kicks in above 100 parts and misreads some holes as separate exterior
  parts) - inflating the exported feature's part count citywide even though the
  underlying data was always correct. Stripping sub-`min_area` holes here, the same
  way sub-`min_area` whole parts are already stripped below, removes the slivers
  before they ever reach export - confirmed on PUMA 4105: 4,926 holes -> 0, area
  changes by 6.9 sq ft (0.0000013%).

  Features left with no qualifying part come back empty rather than NULL, so callers
  can drop them with `WHERE NOT st_isempty(geom)`.
-#}
st_multi(coalesce(
        (
            SELECT st_union(kept.geom)
            FROM (
                SELECT (st_dump(st_collectionextract(
                    st_difference(
                        {{ geom_column }},
                        coalesce(water.geom, st_setsrid('POLYGON EMPTY'::geometry, st_srid({{ geom_column }})))
                    ), 3
                ))).geom AS part_geom
            ) AS parts
            CROSS JOIN LATERAL (
                SELECT st_makepolygon(
                    st_exteriorring(parts.part_geom),
                    coalesce(
                        array_agg(hole.ring) FILTER (WHERE st_area(st_makepolygon(hole.ring)) >= {{ min_area }}),
                        ARRAY[]::geometry[]
                    )
                ) AS geom
                FROM (
                    SELECT st_interiorringn(parts.part_geom, gs) AS ring
                    FROM generate_series(1, st_numinteriorrings(parts.part_geom)) AS gs
                ) AS hole
            ) AS kept
            WHERE st_area(parts.part_geom) >= {{ min_area }}
        ),
        st_setsrid('MULTIPOLYGON EMPTY'::geometry, st_srid({{ geom_column }}))
    ))
{%- endmacro %}
