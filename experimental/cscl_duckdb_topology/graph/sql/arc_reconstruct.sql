-- Not part of the graph build: rebuilds each canonical arc's linearized curve in DuckDB
-- from its 3 control points, mirroring PostGIS's ST_CurveToLine(circ, 0.00025, 1)
-- (liblwgeom lw_arc_center + lwarc_linearize, max-deviation tolerance, no flags), for
-- comparison with the curve carried over from PG. Differences from liblwgeom: angles
-- are a1 + k*increment rather than a running sum, and the libm is macOS's, not glibc's,
-- so last-ulp differences (and, at a boundary, one point more or fewer) are expected.
WITH canon AS (
    SELECT DISTINCT ON (least(node_a, node_b), greatest(node_a, node_b))
        least(node_a, node_b) AS node_lo,
        greatest(node_a, node_b) AS node_hi,
        CASE WHEN node_a < node_b THEN a_x ELSE b_x END AS x1,
        CASE WHEN node_a < node_b THEN a_y ELSE b_y END AS y1,
        mid_x AS x2,
        mid_y AS y2,
        CASE WHEN node_a < node_b THEN b_x ELSE a_x END AS x3,
        CASE WHEN node_a < node_b THEN b_y ELSE a_y END AS y3
    FROM raw_edges
    WHERE is_arc AND node_b IS NOT NULL AND node_a != node_b
    ORDER BY least(node_a, node_b), greatest(node_a, node_b), atomicid
),

-- lw_arc_center
centre AS (
    SELECT
        *,
        x1 + (h21 * dy31 - h31 * dy21) / d AS cx,
        y1 - (h21 * dx31 - h31 * dx21) / d AS cy,
        abs(d) < 1e-8 AS collinear,
        -- lw_segment_side(p1, p3, p2): -1 => clockwise sweep
        sign((x2 - x1) * (y3 - y1) - (x3 - x1) * (y2 - y1)) AS p2_side
    FROM (
        SELECT
            *,
            pow(x2 - x1, 2) + pow(y2 - y1, 2) AS h21,
            pow(x3 - x1, 2) + pow(y3 - y1, 2) AS h31,
            2 * ((x2 - x1) * (y3 - y1) - (x3 - x1) * (y2 - y1)) AS d,
            x2 - x1 AS dx21,
            y2 - y1 AS dy21,
            x3 - x1 AS dx31,
            y3 - y1 AS dy31
        FROM canon
    )
),

angles AS (
    SELECT
        *,
        p2_side = -1 AS clockwise,
        -- angle_increment_using_max_deviation
        2 * acos(1.0 - least(0.00025, 2 * r) / r) AS inc0,
        atan2(y1 - cy, x1 - cx) AS a1,
        atan2(y3 - cy, x3 - cx) AS a3_raw
    FROM (SELECT *, sqrt(pow(cx - x1, 2) + pow(cy - y1, 2)) AS r FROM centre)
),

sweep AS (
    SELECT
        *,
        least(inc0, total) * CASE WHEN clockwise THEN -1 ELSE 1 END AS inc,
        CASE
            WHEN clockwise AND a1 < a3_raw THEN a3_raw - 2 * pi()
            WHEN NOT clockwise AND a1 > a3_raw THEN a3_raw + 2 * pi()
            ELSE a3_raw
        END AS a3
    FROM (
        SELECT
            *,
            CASE
                WHEN (CASE WHEN clockwise THEN a1 - a3_raw ELSE a3_raw - a1 END) <= 0
                    THEN (CASE WHEN clockwise THEN a1 - a3_raw ELSE a3_raw - a1 END) + 2 * pi()
                ELSE (CASE WHEN clockwise THEN a1 - a3_raw ELSE a3_raw - a1 END)
            END AS total
        FROM angles
    )
)

SELECT
    node_lo,
    node_hi,
    collinear OR p2_side = 0 AS collinear,
    CASE
        -- lwcircstring_linearize falls back to p1, p2, p3 for a collinear "arc"
        WHEN collinear OR p2_side = 0
            THEN st_makeline([st_point(x1, y1), st_point(x2, y2), st_point(x3, y3)])
        ELSE st_makeline(list_concat(
            [st_point(x1, y1)],
            [
                st_point(cx + r * cos(a1 + k * inc), cy + r * sin(a1 + k * inc))
                FOR k IN range(1, (ceil(abs(a3 - a1) / abs(inc)) + 2)::bigint)
                IF CASE WHEN clockwise THEN a1 + k * inc > a3 ELSE a1 + k * inc < a3 END
            ],
            [st_point(x3, y3)]
        ))
    END AS geom
FROM sweep
