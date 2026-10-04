{{ config(
    materialized='table'
) }}

-- Maps every exact point (int__topology__exact_points) to its canonical node id. Three
-- steps, each scaling linearly:
--   1. int__topology__grid_cells has already grid-snapped the points (a GROUP BY).
--   2. cell_merges: for each cell, its 8 neighbours by integer offset (an equality
--      join, so hashable), confirmed with ST_DWithin at the merge tolerance.
--   3. reachable/components: connected components over those merges, so merging is
--      transitive (A~B and B~C merge A, B, C even if A and C aren't within tolerance).
--      Merge clusters are tiny (a few near-coincident copies of one corner), so the
--      recursion is bounded by cluster size, not dataset size.
-- node_id is global - a node is routinely shared by AtomicPolygons in different
-- districts.

WITH RECURSIVE cell_ids AS (
    SELECT
        row_number() OVER (ORDER BY x_cell, y_cell) AS cell_id,
        x_cell,
        y_cell,
        geom,
        exact_point_ids
    FROM {{ ref('int__topology__grid_cells') }}
),

-- The 3x3 neighbourhood as 9 explicit (dx, dy) offsets joined by equality: a BETWEEN
-- range join can't be hashed and plans as a nested loop over all pairs of cells.
-- MATERIALIZED so the recursive step joins against a fixed table.
cell_merges AS MATERIALIZED (
    SELECT
        a.cell_id AS cell_a,
        b.cell_id AS cell_b
    FROM cell_ids AS a
    CROSS JOIN generate_series(-1, 1) AS dx
    CROSS JOIN generate_series(-1, 1) AS dy
    INNER JOIN cell_ids AS b
        ON
            b.x_cell = a.x_cell + dx
            AND b.y_cell = a.y_cell + dy
    WHERE
        a.cell_id != b.cell_id
        AND st_dwithin(a.geom, b.geom, {{ var('ap_topology_node_merge_tolerance_ft') }})
),

-- Only cells that merge with something need the recursive walk; every other cell is
-- its own component (the coalesce in `components`).
reachable (start_cell, reached_cell) AS (
    SELECT DISTINCT
        cell_a,
        cell_a  -- noqa: AL08
    FROM cell_merges
    UNION
    SELECT
        r.start_cell,
        m.cell_b
    FROM reachable AS r
    INNER JOIN cell_merges AS m ON r.reached_cell = m.cell_a
),

components AS (
    SELECT
        ci.cell_id,
        coalesce(min(r.reached_cell), ci.cell_id) AS component_id
    FROM cell_ids AS ci
    LEFT JOIN reachable AS r ON ci.cell_id = r.start_cell
    GROUP BY ci.cell_id
),

node_ids AS (
    SELECT
        component_id,
        row_number() OVER (ORDER BY component_id) AS node_id
    FROM (SELECT DISTINCT component_id FROM components) AS distinct_components
)

SELECT
    unnest(ci.exact_point_ids) AS exact_point_id,
    n.node_id
FROM cell_ids AS ci
INNER JOIN components AS c ON ci.cell_id = c.cell_id
INNER JOIN node_ids AS n ON c.component_id = n.component_id
