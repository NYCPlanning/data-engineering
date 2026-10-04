{#-
  District boundaries built from the AtomicPolygon topology. Every district type is the
  same five steps, parameterized only by which int__topology__ap_entities column holds
  the district id:

    district_boundary_classify_edges    -> int__boundary__<district>_edges
    district_boundary_build_raw         -> int__boundary__<district>_raw (face membership)
    district_boundary_strip_noise_rings -> int__boundary__<district>
    district_boundary_validity          -> qa__boundary__<district>_validity
    district_boundary_vs_prod           -> qa__boundary__<district>_vs_prod

  Every step outputs `entity_id` as its key. No step touches the topology itself
  (int__topology__edges is shared), so adding a district type costs only these steps.
  No polygon overlay on source polygons is on the construction path: boundaries are
  assembled from classified, noded topology edges.
-#}


{% macro district_boundary_classify_edges(entity_column, include_assigned_water=false) %}
{#-
  Classifies every edge in int__topology__edges as 'interior' or 'exterior' to each
  district that owns it, for building a land boundary:

    - land-water edge (one owner water, the other land): always 'exterior' - it's the
      shoreline, whichever district the water is tagged with. Emitted once per land
      owner, never also through the land-land path (a boundary line present twice
      breaks ST_BuildArea's parity-based polygon building).
    - water-water edge: excluded.
    - land-land edge: 'interior' only when ALL its owners belong to the same district
      and there are at least 2 of them; otherwise 'exterior'.

  include_assigned_water: when true, a water AP that carries a district id counts as
  land for that district; only unassigned water acts as shoreline. Off for census
  geographies (land-only); on for fire divisions, whose published layer includes the
  water APs that have an administering fire company.

  APs with a NULL district still count as owners (so their neighbours' shared edges
  read as 'exterior') but produce no rows of their own.
-#}
WITH edges AS (
    SELECT
        e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, m.{{ entity_column }} AS entity_id,
        {%- if include_assigned_water %}
        e.water_flag = '1' AND m.{{ entity_column }} IS NULL AS is_water
        {%- else %}
        e.water_flag = '1' AS is_water
        {%- endif %}
    FROM {{ ref('int__topology__edges') }} AS e
    INNER JOIN {{ ref('int__topology__ap_entities') }} AS m ON e.atomicid = m.atomicid
),

has_water_neighbor AS (
    SELECT DISTINCT node_lo, node_hi, is_arc
    FROM edges
    WHERE is_water
),

land_land_edges AS (
    SELECT e.node_lo, e.node_hi, e.is_arc, e.geom, e.atomicid, e.entity_id
    FROM edges AS e
    LEFT JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water AND hw.node_lo IS NULL
),

edge_owner_counts AS (
    SELECT node_lo, node_hi, is_arc, count(DISTINCT atomicid) AS total_owners
    FROM land_land_edges
    GROUP BY node_lo, node_hi, is_arc
),

edge_entity_counts AS (
    SELECT
        node_lo, node_hi, is_arc, entity_id,
        count(DISTINCT atomicid) AS owners_in_entity,
        (array_agg(geom))[1] AS geom
    FROM land_land_edges
    GROUP BY node_lo, node_hi, is_arc, entity_id
),

land_land_classified AS (
    SELECT
        ec.entity_id, ec.node_lo, ec.node_hi, ec.is_arc, ec.geom,
        CASE
            WHEN ec.owners_in_entity = eoc.total_owners AND eoc.total_owners >= 2 THEN 'interior'
            ELSE 'exterior'
        END AS edge_type
    FROM edge_entity_counts AS ec
    INNER JOIN edge_owner_counts AS eoc
        ON ec.node_lo = eoc.node_lo AND ec.node_hi = eoc.node_hi AND ec.is_arc = eoc.is_arc
),

-- Emitted once per LAND-side row, using THAT row's own entity - "is this land parcel's
-- edge exterior here because it borders water" doesn't depend on which entity the
-- water itself is tagged with, so this intentionally does NOT require the land and
-- water owner to share an entity.
shoreline_edges AS (
    SELECT
        e.entity_id,
        e.node_lo,
        e.node_hi,
        e.is_arc,
        e.geom,
        'exterior' AS edge_type
    FROM edges AS e
    INNER JOIN has_water_neighbor AS hw
        ON e.node_lo = hw.node_lo AND e.node_hi = hw.node_hi AND e.is_arc = hw.is_arc
    WHERE NOT e.is_water
)

-- An AP with no entity (e.g. no fire division) still counts as an owner above, so its
-- neighbors' shared edges correctly read as 'exterior' - it just builds no boundary.
SELECT * FROM land_land_classified WHERE entity_id IS NOT NULL
UNION ALL
SELECT * FROM shoreline_edges WHERE entity_id IS NOT NULL
{% endmacro %}


{% macro district_boundary_build_raw(edges_model, entity_column, include_assigned_water=false) %}
{#-
  One row per district, built from its 'exterior' edges. "_raw": noise interior rings
  are not yet removed (district_boundary_strip_noise_rings does that, and
  district_boundary_validity inspects this pre-cleanup geometry).

  Face membership, not ring parity: the exterior edges are polygonized into faces, and
  a face is kept only if it contains the interior point of one of the district's own
  APs (same land / assigned-water rule as district_boundary_classify_edges). ST_BuildArea
  decides inside vs outside by ring nesting, which fails where a district's outline
  pinches - e.g. hurricane evacuation zone 5 wraps a zone-6 block that touches zone 5's
  outer boundary at single corner points, and ST_BuildArea returned zone 5 with no hole,
  swallowing the block. A face either is or isn't covered by the district's APs,
  however its rings touch. Faces holding no member AP at all (slivers between two APs'
  mismatched corners) drop out here too.

  Each district is built from its own edge classification rather than by unioning
  smaller built districts - an edge between two tracts in the same PUMA is 'interior'
  at PUMA level but 'exterior' at tract level.
-#}
WITH faces AS (
    SELECT
        entity_id,
        (st_dump(st_polygonize(geom))).geom AS face
    FROM {{ ref(edges_model) }}
    WHERE edge_type = 'exterior'
    GROUP BY entity_id
),

member_points AS (
    SELECT
        m.{{ entity_column }} AS entity_id,
        st_pointonsurface(a.geom) AS pt
    FROM {{ ref('int__topology__ap_entities') }} AS m
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON m.atomicid = a.atomicid
    WHERE
        m.{{ entity_column }} IS NOT NULL
        {%- if not include_assigned_water %}
        AND m.water_flag != '1'
        {%- endif %}
),

kept AS (
    SELECT
        f.entity_id,
        f.face
    FROM faces AS f
    WHERE EXISTS (
        SELECT 1 FROM member_points AS mp
        WHERE mp.entity_id = f.entity_id AND f.face && mp.pt AND st_contains(f.face, mp.pt)
    )
)

-- kept faces share exact, already-noded edges, so this union only dissolves them
SELECT
    entity_id,
    st_union(face) AS geom
FROM kept
GROUP BY entity_id
{% endmacro %}


{% macro district_boundary_strip_noise_rings(boundary_raw_model, edges_model) %}
{#-
  Removes interior rings that are noise, keeps real ones. A ring is noise if any of its
  edges is missing a left or right AtomicPolygon - a single owner across the whole
  topology, not just within this district. Noise rings come from two AtomicPolygons
  disagreeing about a shared corner (thin loops of one-owner edges, often near-zero area
  but long). A real hole - an enclave district, or water - is bounded on both sides:
  this district's AP on one, whatever fills the hole on the other.

  Ring edges are identified with ST_Covers, which is exact here because ST_BuildArea
  reuses the input edges' coordinates.
-#}
WITH owner_counts AS (
    SELECT node_lo, node_hi, is_arc, count(DISTINCT atomicid) AS total_owners
    FROM {{ ref('int__topology__edges') }}
    GROUP BY node_lo, node_hi, is_arc
),

one_owner_edges AS (
    SELECT e.entity_id, e.geom
    FROM {{ ref(edges_model) }} AS e
    INNER JOIN owner_counts AS oc
        ON e.node_lo = oc.node_lo AND e.node_hi = oc.node_hi AND e.is_arc = oc.is_arc
    WHERE e.edge_type = 'exterior' AND oc.total_owners = 1
),

parts AS (
    SELECT
        r.entity_id,
        -- ST_Dump of a single Polygon has an empty path - NULL would never join below
        coalesce(d.path[1], 1) AS part_n,
        d.geom AS part
    FROM {{ ref(boundary_raw_model) }} AS r
    CROSS JOIN LATERAL st_dump(r.geom) AS d
),

interior_rings AS (
    SELECT
        p.entity_id,
        p.part_n,
        n AS ring_n,
        st_interiorringn(p.part, n) AS ring
    FROM parts AS p
    CROSS JOIN LATERAL generate_series(1, st_numinteriorrings(p.part)) AS n
),

kept_rings AS (
    SELECT
        ir.entity_id,
        ir.part_n,
        array_agg(ir.ring ORDER BY ir.ring_n) AS rings
    FROM interior_rings AS ir
    WHERE NOT EXISTS (
        SELECT 1
        FROM one_owner_edges AS e
        WHERE e.entity_id = ir.entity_id AND e.geom && ir.ring AND st_covers(ir.ring, e.geom)
    )
    GROUP BY ir.entity_id, ir.part_n
),

rebuilt AS (
    SELECT
        p.entity_id,
        CASE
            WHEN k.rings IS NULL THEN st_makepolygon(st_exteriorring(p.part))
            ELSE st_makepolygon(st_exteriorring(p.part), k.rings)
        END AS geom
    FROM parts AS p
    LEFT JOIN kept_rings AS k ON p.entity_id = k.entity_id AND p.part_n = k.part_n
)

SELECT
    entity_id,
    st_union(geom) AS geom
FROM rebuilt
GROUP BY entity_id
{% endmacro %}


{% macro district_boundary_validity(edges_model, boundary_raw_model, boundary_model) %}
{#-
  Topology checks per district. Three gate is_valid:
    even_degree_ok      - every node on the district's 'exterior' edges has even degree
                          (closed rings; an odd node is a dangling chain).
    buildarea_ok        - ST_BuildArea returned a non-empty polygonal result.
    final_geom_valid_ok - ST_IsValid on the final (noise-stripped) boundary.
  One diagnostic only:
    is_simple_ok        - the raw 'exterior' edges, line-merged, are simple. False for
                          a district carrying a noise loop that the final geometry
                          correctly drops, so it doesn't gate; useful when
                          investigating a new failure.
-#}
WITH exterior AS (
    SELECT entity_id, node_lo, node_hi, geom
    FROM {{ ref(edges_model) }}
    WHERE edge_type = 'exterior'
),

merged_check AS (
    SELECT
        entity_id,
        st_issimple(st_linemerge(st_collect(geom))) AS is_simple_ok
    FROM exterior
    GROUP BY entity_id
),

node_degree AS (
    SELECT
        entity_id,
        node_id,
        count(*) AS degree
    FROM (
        SELECT entity_id, node_lo AS node_id FROM exterior
        UNION ALL
        SELECT entity_id, node_hi AS node_id FROM exterior
    ) AS endpoints
    GROUP BY entity_id, node_id
),

degree_check AS (
    SELECT
        entity_id,
        count(*) FILTER (WHERE degree % 2 != 0) AS n_odd_degree_nodes,
        count(*) AS n_nodes_total
    FROM node_degree
    GROUP BY entity_id
),

buildarea_check AS (
    SELECT
        entity_id,
        geom IS NOT NULL
        AND NOT st_isempty(geom)
        AND st_geometrytype(geom) IN ('ST_Polygon', 'ST_MultiPolygon') AS buildarea_ok
    FROM {{ ref(boundary_raw_model) }}
),

final_geom_check AS (
    SELECT
        entity_id,
        geom IS NOT NULL AND st_isvalid(geom) AS final_geom_valid_ok
    FROM {{ ref(boundary_model) }}
)

SELECT
    dc.entity_id,
    dc.n_odd_degree_nodes = 0 AS even_degree_ok,
    dc.n_odd_degree_nodes,
    dc.n_nodes_total,
    coalesce(mc.is_simple_ok, FALSE) AS is_simple_ok,
    coalesce(bc.buildarea_ok, FALSE) AS buildarea_ok,
    coalesce(fc.final_geom_valid_ok, FALSE) AS final_geom_valid_ok,
    (dc.n_odd_degree_nodes = 0)
    AND coalesce(bc.buildarea_ok, FALSE)
    AND coalesce(fc.final_geom_valid_ok, FALSE) AS is_valid
FROM degree_check AS dc
LEFT JOIN merged_check AS mc ON dc.entity_id = mc.entity_id
LEFT JOIN buildarea_check AS bc ON dc.entity_id = bc.entity_id
LEFT JOIN final_geom_check AS fc ON dc.entity_id = fc.entity_id
{% endmacro %}


{% macro district_boundary_vs_prod(boundary_model, validity_model, prod_identifier, prod_key, entity_column, dissolve_prod=false) %}
{#-
  Compares a built boundary against its published production_outputs layer: symdiff
  area %, perimeter %, part and hole counts, Hausdorff (selectively), with the
  district's validity verdict alongside.

  Full outer join, prod restricted to district ids present in scope, with match_status
  'matched' / 'built_only' / 'prod_only' - a district missing on one side is a result,
  not something to drop silently.

  Hausdorff catches spurs and thin loops that area and perimeter miss, but GEOS's
  implementation is O(n*m) in vertex count and can't be interrupted mid-call. It's
  computed only when either side has an interior ring or the part counts differ, and
  only below boundary_qa_hausdorff_max_vertex_product; NULL otherwise.

  dissolve_prod: for published layers that store one feature per disjoint piece rather
  than one per district (e.g. fgdb_nymcea), union prod's features per key first.
-#}
WITH in_scope AS (
    SELECT DISTINCT {{ entity_column }} AS entity_id
    FROM {{ ref('int__topology__ap_entities') }}
),

prod AS (
    SELECT
        p.{{ prod_key }} AS entity_id,
        {% if dissolve_prod %}st_union(linearize(p.shape)){% else %}linearize(p.shape){% endif %} AS geom
    FROM {{ adapter.get_relation(database="db-cscl", schema="production_outputs", identifier=prod_identifier) }} AS p
    INNER JOIN in_scope AS s ON p.{{ prod_key }} = s.entity_id
    {% if dissolve_prod %}GROUP BY p.{{ prod_key }}{% endif %}
),

structure AS (
    SELECT
        entity_id,
        geom,
        st_numgeometries(geom) AS n_parts,
        (SELECT sum(st_numinteriorrings(d.geom)) FROM st_dump(geom) AS d) AS n_holes
    FROM {{ ref(boundary_model) }}
),

prod_structure AS (
    SELECT
        entity_id,
        geom,
        st_numgeometries(geom) AS n_parts,
        (SELECT sum(st_numinteriorrings(d.geom)) FROM st_dump(geom) AS d) AS n_holes
    FROM prod
)

SELECT
    coalesce(p.entity_id, b.entity_id) AS entity_id,
    CASE
        WHEN b.entity_id IS NULL THEN 'prod_only'
        WHEN p.entity_id IS NULL THEN 'built_only'
        ELSE 'matched'
    END AS match_status,
    v.is_valid,
    b.n_parts AS built_parts,
    p.n_parts AS prod_parts,
    b.n_holes AS built_holes,
    p.n_holes AS prod_holes,
    round((st_area(st_symdifference(b.geom, p.geom)) / nullif(st_area(p.geom), 0) * 100)::numeric, 6) AS symdiff_area_pct,
    round(((st_perimeter(b.geom) - st_perimeter(p.geom)) / nullif(st_perimeter(p.geom), 0) * 100)::numeric, 4) AS perimeter_pct_diff,
    CASE
        WHEN
            (b.n_holes > 0 OR p.n_holes > 0 OR b.n_parts != p.n_parts)
            AND st_npoints(b.geom)::bigint * st_npoints(p.geom) <= {{ var('boundary_qa_hausdorff_max_vertex_product') }}
            THEN round(st_hausdorffdistance(b.geom, p.geom)::numeric, 3)
    END AS hausdorff_ft,
    round(st_area(b.geom)::numeric, 1) AS built_area_sqft,
    round(st_area(p.geom)::numeric, 1) AS prod_area_sqft
FROM prod_structure AS p
FULL OUTER JOIN structure AS b ON p.entity_id = b.entity_id
LEFT JOIN {{ ref(validity_model) }} AS v ON coalesce(p.entity_id, b.entity_id) = v.entity_id
{% endmacro %}


{% macro district_boundary_outliers(entity, entity_column, prod_identifier, prod_key) %}
{#-
  For viewing (QGIS etc.): every district missing on one side, or whose Hausdorff (where
  computed) or symdiff exceeds boundary_qa_outlier_min_hausdorff_ft /
  boundary_qa_outlier_min_symdiff_pct, broken into layers by `layer`:
    built / prod  - the two boundaries
    built_only    - built minus prod
    prod_only     - prod minus built
    ap            - every AtomicPolygon in the district, with water_flag
  ST_Difference is fine here: this is diagnostics, not the construction path.
-#}
WITH outliers AS (
    SELECT entity_id, hausdorff_ft, symdiff_area_pct
    FROM {{ ref('qa__boundary__' ~ entity ~ '_vs_prod') }}
    WHERE
        match_status != 'matched'
        OR hausdorff_ft > {{ var('boundary_qa_outlier_min_hausdorff_ft') }}
        OR symdiff_area_pct > {{ var('boundary_qa_outlier_min_symdiff_pct') }}
),

built AS (
    SELECT b.entity_id, b.geom
    FROM {{ ref('int__boundary__' ~ entity) }} AS b
    INNER JOIN outliers AS o ON b.entity_id = o.entity_id
),

prod AS (
    SELECT p.{{ prod_key }} AS entity_id, linearize(p.shape) AS geom
    FROM {{ adapter.get_relation(database="db-cscl", schema="production_outputs", identifier=prod_identifier) }} AS p
    INNER JOIN outliers AS o ON p.{{ prod_key }} = o.entity_id
),

layers AS (
    SELECT entity_id, 'built' AS layer, NULL AS atomicid, NULL AS water_flag, geom FROM built
    UNION ALL
    SELECT entity_id, 'prod', NULL, NULL, geom FROM prod
    UNION ALL
    SELECT b.entity_id, 'built_only', NULL, NULL, st_difference(b.geom, p.geom)
    FROM built AS b INNER JOIN prod AS p ON b.entity_id = p.entity_id
    UNION ALL
    SELECT b.entity_id, 'prod_only', NULL, NULL, st_difference(p.geom, b.geom)
    FROM built AS b INNER JOIN prod AS p ON b.entity_id = p.entity_id
    UNION ALL
    SELECT m.{{ entity_column }}, 'ap', a.atomicid, a.water_flag, a.geom
    FROM {{ ref('int__topology__ap_entities') }} AS m
    INNER JOIN outliers AS o ON m.{{ entity_column }} = o.entity_id
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON m.atomicid = a.atomicid
)

SELECT
    l.entity_id,
    l.layer,
    l.atomicid,
    l.water_flag,
    o.hausdorff_ft,
    o.symdiff_area_pct,
    round(st_area(l.geom)::numeric, 1) AS area_sqft,
    st_multi(st_collectionextract(l.geom, 3))::geometry(MultiPolygon, 2263) AS geom
FROM layers AS l
INNER JOIN outliers AS o ON l.entity_id = o.entity_id
WHERE NOT st_isempty(l.geom)
{% endmacro %}
