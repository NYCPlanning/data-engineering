-- DuckDB functions used by the models, created at run start (dbt_project.yml on-run-start).
-- Requires the spatial extension (INSTALL spatial; LOAD spatial;).
-- Geometry carries no SRID in DuckDB. geometry_diff_metrics (QA only) is a dbt macro in
-- macros/geometry_diff_qa.sql.

-- format_lion_text: lpad/rpad to n characters (truncates on the right).
-- A NULL value becomes '' and, if blank_if_none, the fill becomes a space.
{% macro create_duckdb_functions() %}
CREATE OR REPLACE MACRO format_lion_text(value, n, fill := '0', blank_if_none := FALSE, left_ := FALSE) AS
CASE
    WHEN left_
        THEN rpad(
            coalesce(value, ''), n, CASE WHEN value IS NULL AND blank_if_none THEN ' ' ELSE fill END
        )
    ELSE lpad(
        coalesce(value, ''), n, CASE WHEN value IS NULL AND blank_if_none THEN ' ' ELSE fill END
    )
END;

-- convert_level_code: level 1-26 -> A-Z; 99 or non-street features -> '*'; shoreline -> '$'.
CREATE OR REPLACE MACRO convert_level_code(level_code, feature_type) AS
CASE
    WHEN feature_type = 'shoreline' THEN '$'
    WHEN feature_type = 'nonstreetfeatures' OR level_code = 99 THEN '*'
    WHEN level_code BETWEEN 1 AND 26 THEN chr(64 + level_code::integer)
END;

-- find_circle: POINT Z (center_x, center_y, radius) of the circle through three points,
-- or NULL if they are collinear.
CREATE OR REPLACE MACRO find_circle(p1, p2, p3) AS (
    SELECT
        CASE
            WHEN g = 0 THEN NULL
            ELSE st_point3d(
                (d * e - b * f) / g,
                (a * f - c * e) / g,
                sqrt(
                    power(x1 - (d * e - b * f) / g, 2) + power(y1 - (a * f - c * e) / g, 2)
                )
            )::geometry
        END
    FROM (
        SELECT
            x1,
            y1,
            a,
            b,
            c,
            d,
            e,
            f,
            2.0 * (a * (st_y(p3) - st_y(p2)) - b * (st_x(p3) - st_x(p2))) AS g
        FROM (
            SELECT
                x1,
                y1,
                a,
                b,
                c,
                d,
                a * (x1 + st_x(p2)) + b * (y1 + st_y(p2)) AS e,
                c * (x1 + st_x(p3)) + d * (y1 + st_y(p3)) AS f
            FROM (
                SELECT
                    st_x(p1) AS x1,
                    st_y(p1) AS y1,
                    st_x(p2) - st_x(p1) AS a,
                    st_y(p2) - st_y(p1) AS b,
                    st_x(p3) - st_x(p1) AS c,
                    st_y(p3) - st_y(p1) AS d
            )
        )
    )
);

-- offset_points: STRUCT(left_offset_point, right_offset_point), points offset_length either
-- side of the point at half the line's length, along the normal of the vertex-to-vertex
-- piece of the line that point is on (prod: IConstructPoint.ConstructOffset at ratio 0.5).
-- Found by walking the line by length (its first half, st_linesubstring), not by nearness to
-- the midpoint: a line that doubles back passes through its midpoint more than once. A half
-- point on a vertex takes the piece ending there. NULL if line is not a LineString.
-- (Pure expressions, no subqueries: a macro whose subqueries reference its arguments
-- can't be called on table columns. [x] + list_transform binds an intermediate once.)
CREATE OR REPLACE MACRO _half_length_piece(line) AS (
    list_transform(
        [st_linesubstring(line, 0, 0.5)],
        h -> list_transform(
            [st_endpoint(h)],
            -- the last vertex before the half point that isn't (to 1e-6 ft) the half point
            -- itself: a half point a rounding error past a vertex (an arc's middle vertex)
            -- would otherwise leave a piece too short to give a direction
            e -> struct_pack(
                p1 := st_pointn(h, list_max(list_filter(
                    range(1, st_npoints(h)),
                    i -> st_distance(st_pointn(h, i::INTEGER), e) > 1e-6
                ))::INTEGER),
                p2 := e
            )
        )[1]
    )[1]
);

CREATE OR REPLACE MACRO offset_points(line, offset_length := 1.0) AS
CASE
    WHEN st_geometrytype(line) <> 'LINESTRING' THEN NULL
    ELSE list_transform(
        [_half_length_piece(line)],
        seg -> list_transform(
            [struct_pack(dx := st_y(seg.p2) - st_y(seg.p1), dy := st_x(seg.p1) - st_x(seg.p2))],
            n -> struct_pack(
                left_offset_point := st_point(
                    st_x(seg.p2) - n.dx * offset_length / sqrt(n.dx * n.dx + n.dy * n.dy),
                    st_y(seg.p2) - n.dy * offset_length / sqrt(n.dx * n.dx + n.dy * n.dy)
                ),
                right_offset_point := st_point(
                    st_x(seg.p2) + n.dx * offset_length / sqrt(n.dx * n.dx + n.dy * n.dy),
                    st_y(seg.p2) + n.dy * offset_length / sqrt(n.dx * n.dx + n.dy * n.dy)
                )
            )
        )[1]
    )[1]
END;

-- linearize: curves are linearized at load time (dcpy.geospatial.curves), so all that's
-- left is merging MultiLineStrings.
CREATE OR REPLACE MACRO linearize(geom) AS
CASE WHEN st_geometrytype(geom) = 'MULTILINESTRING' THEN st_linemerge(geom) ELSE geom END;
-- dump_points: every vertex of a (multi)polygon, as a list to UNNEST - one struct per
-- vertex with its path: part (1-based member), ring (1 = exterior), point
-- (1-based, closing point included). Unnest it in the select list (SELECT ...,
-- unnest(dump_points(g)) AS d); a lateral unnest copies the whole input row per vertex.
CREATE OR REPLACE MACRO ring_n(p, r) AS
CASE WHEN r = 0 THEN st_exteriorring(p) ELSE st_interiorringn(p, r::INTEGER) END;

CREATE OR REPLACE MACRO dump_points(g) AS (
    -- each ring is extracted once and its vertices dumped in one pass (per-vertex
    -- st_pointn on the ring is quadratic on large rings)
    flatten(list_transform(st_dump(g), p -> flatten(
        list_transform(range(0, st_numinteriorrings(p.geom) + 1), r -> list_transform(
            st_dump(st_points(ring_n(p.geom, r))),
            q -> {
                'part': coalesce(p.path[1], 1)::INTEGER, 'ring': (r + 1)::INTEGER,
                'point': q.path[1]::INTEGER, 'geom': q.geom
            }
        ))
    )))
);
-- linearize_arc: the arc p0 -> pm -> p1 as a LINESTRING, the same algorithm as
-- dcpy.geospatial.curves.linearize_arc (the load-time linearizer): equal angular steps
-- with sagitta <= tol, at least 2 segments, endpoints exact, and symmetric - vertices are
-- generated from the lexicographically smaller endpoint, so reversing the arc reverses
-- the vertex list exactly. Collinear points give the straight segment. Not for full
-- circles (p0 = p1).
CREATE OR REPLACE MACRO _linearize_arc_fwd(x0, y0, xm, ym, x1, y1, tol) AS (
    SELECT
        CASE
            WHEN c.d = 0 THEN st_makeline(st_point(x0, y0), st_point(x1, y1))
            WHEN w.sweep < pi() AND 2.0 * c.r * pow(sin(w.sweep / 4.0), 2) < tol
                THEN st_makeline(st_point(x0, y0), st_point(x1, y1))
            ELSE st_makeline(
                [st_point(x0, y0)]
                || list_transform(
                    range(1, w.n),
                    k -> st_point(
                        c.cx + c.r * cos(w.a0 + w.sgn * w.sweep / w.n * k),
                        c.cy + c.r * sin(w.a0 + w.sgn * w.sweep / w.n * k)
                    )
                )
                || [st_point(x1, y1)]
            )
        END
    FROM (
        SELECT
            d,
            x0 + ux AS cx,
            y0 + uy AS cy,
            sqrt(ux * ux + uy * uy) AS r
        FROM (
            SELECT
                d,
                CASE WHEN d != 0 THEN ((y1 - y0) * b2 - (ym - y0) * c2) / d END AS ux,
                CASE WHEN d != 0 THEN ((xm - x0) * c2 - (x1 - x0) * b2) / d END AS uy
            FROM (
                SELECT
                    2.0 * ((xm - x0) * (y1 - y0) - (ym - y0) * (x1 - x0)) AS d,
                    (xm - x0) * (xm - x0) + (ym - y0) * (ym - y0) AS b2,
                    (x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0) AS c2
            )
        )
    ) AS c,
    LATERAL (
        SELECT
            a0,
            sweep,
            sgn,
            CASE
                WHEN c.r <= tol / 2 THEN 2
                ELSE greatest(2, least(100000, ceil(sweep / (4.0 * asin(sqrt(tol / (2.0 * c.r))))))::INTEGER)
            END AS n
        FROM (
            SELECT
                a0,
                CASE WHEN cross_ > 0 THEN (((a0 - a1) % (2 * pi())) + 2 * pi()) % (2 * pi())
                    ELSE (((a1 - a0) % (2 * pi())) + 2 * pi()) % (2 * pi()) END AS sweep,
                CASE WHEN cross_ > 0 THEN -1.0 ELSE 1.0 END AS sgn
            FROM (
                SELECT
                    atan2(y0 - c.cy, x0 - c.cx) AS a0,
                    atan2(y1 - c.cy, x1 - c.cx) AS a1,
                    (x1 - x0) * (ym - y0) - (y1 - y0) * (xm - x0) AS cross_
            )
        )
    ) AS w
);

CREATE OR REPLACE MACRO linearize_arc(x0, y0, xm, ym, x1, y1, tol := 0.00025) AS
CASE
    WHEN x1 < x0 OR (x1 = x0 AND y1 < y0)
        THEN st_reverse(_linearize_arc_fwd(x1, y1, xm, ym, x0, y0, tol))
    ELSE _linearize_arc_fwd(x0, y0, xm, ym, x1, y1, tol)
END;
-- hausdorff_distance: GEOS's discrete Hausdorff distance between a and b: the larger of
-- (each vertex of a to the nearest point of b) and the reverse.
CREATE OR REPLACE MACRO hausdorff_distance(a, b) AS greatest(
    list_max(list_transform(st_dump(st_points(a)), p -> st_distance(p.geom, b))),
    list_max(list_transform(st_dump(st_points(b)), p -> st_distance(p.geom, a)))
);
{% endmacro %}
