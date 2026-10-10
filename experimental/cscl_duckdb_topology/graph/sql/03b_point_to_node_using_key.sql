-- int__topology__point_to_node, alternative: same merges, but connected components by
-- min-label propagation with a USING KEY recursive CTE (one row per cell, overwritten
-- in place) instead of materializing every (start, reached) pair. Same output: each
-- component's label converges to its minimum cell id.
WITH RECURSIVE cell_ids AS (
    SELECT
        row_number() OVER (ORDER BY x_cell, y_cell) AS cell_id,
        x_cell,
        y_cell,
        geom,
        exact_point_ids
    FROM grid_cells
),

-- The 9 neighbour keys are computed in their own CTE so the join below is a plain
-- equality on columns. Written as in PG (ON b.x_cell = a.x_cell + dx ...) DuckDB plans a
-- nested-loop join on `a.cell_id != b.cell_id` over the full cross product and never
-- finishes.
neighbour_keys AS (
    SELECT
        a.cell_id,
        a.geom,
        a.x_cell + gx.dx AS nx,
        a.y_cell + gy.dy AS ny
    FROM cell_ids AS a
    CROSS JOIN generate_series(-1, 1) AS gx (dx)
    CROSS JOIN generate_series(-1, 1) AS gy (dy)
),

cell_merges AS MATERIALIZED (
    SELECT
        k.cell_id AS cell_a,
        b.cell_id AS cell_b
    FROM neighbour_keys AS k
    INNER JOIN cell_ids AS b
        ON b.x_cell = k.nx AND b.y_cell = k.ny
    WHERE
        k.cell_id != b.cell_id
        AND st_dwithin(k.geom, b.geom, ${tol})
),

labels (cell, comp) USING KEY (cell) AS (
    SELECT DISTINCT cell_a, cell_a FROM cell_merges
    UNION ALL
    SELECT m.cell_b, min(l.comp)
    FROM labels AS l
    INNER JOIN cell_merges AS m ON l.cell = m.cell_a
    INNER JOIN recurring.labels AS cur ON m.cell_b = cur.cell
    GROUP BY m.cell_b
    HAVING min(l.comp) < any_value(cur.comp)
),

components AS (
    SELECT
        ci.cell_id,
        coalesce(l.comp, ci.cell_id) AS component_id
    FROM cell_ids AS ci
    LEFT JOIN labels AS l ON ci.cell_id = l.cell
),

node_ids AS (
    SELECT
        component_id,
        row_number() OVER (ORDER BY component_id) AS node_id
    FROM (SELECT DISTINCT component_id FROM components)
)

SELECT
    unnest(ci.exact_point_ids) AS exact_point_id,
    n.node_id
FROM cell_ids AS ci
INNER JOIN components AS c ON ci.cell_id = c.cell_id
INNER JOIN node_ids AS n ON c.component_id = n.component_id
