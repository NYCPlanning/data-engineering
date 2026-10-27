{{ config(
    materialized='table'
) }}

-- Police precinct boundaries: the NYPD source precinct polygons clipped to land - not
-- built from the AtomicPolygon topology, since NYPD's lines don't follow AP edges (they
-- cut through hundreds of APs; see int__topology__ap_police). Land is the union of the
-- topology-built borough boundaries (int__boundary__bb), so the shoreline matches every
-- other layer.
--
-- Clipping a line that runs almost along the shoreline leaves two artefacts:
-- * spikes: a ring runs out along the shoreline and straight back, about 0.0001 ft
--   apart, for up to a few hundred ft (e.g. precincts 41, 72, 88, 110 and 120). They add
--   length but no area. Each is removed by dropping the ring vertices on it, so every
--   vertex left is one the clip produced.
-- * hairline slivers: parts and holes under 100 sq ft are dropped (every sliver observed
--   is under 1 sq ft; real pieces are far larger).
--
-- Finding a spike: open the part (shrink by spike_open_ft, grow back) and take the ring
-- vertices more than spike_vertex_ft from the result; they sit on something narrower than
-- twice spike_open_ft, or at a sharp corner. Take each run of them with the vertices either
-- side of it: the longest stretch that, closed back to its start, encloses less than
-- spike_half_width_ft of area per foot of length is a spike, and its inner vertices go.
-- Spikes measure under 0.0003 ft on that test, the nearest real corner 0.005 ft.
{%- set spike_open_ft = 0.001 %}
{%- set spike_vertex_ft = 0.002 %}
{%- set spike_half_width_ft = 0.001 %}

WITH land AS (
    SELECT st_union_agg(geom) AS geom
    FROM {{ ref('int__boundary__bb') }}
),

clipped AS (
    SELECT
        precinct,
        part,
        row_number() OVER () AS part_id
    FROM (
        SELECT
            p.precinct::smallint AS precinct,
            unnest(st_dump(st_collectionextract(st_intersection(p.geom, land.geom), 3))).geom AS part
        FROM {{ ref('stg__nypdprecinct') }} AS p
        CROSS JOIN land
    )
),

opened AS (
    SELECT
        part_id,
        part,
        st_buffer(st_buffer(part, -{{ spike_open_ft }}), {{ spike_open_ft }}) AS opened_part
    FROM clipped
),

rings AS (
    SELECT
        part_id,
        opened_part,
        0 AS ring_n,
        st_exteriorring(part) AS ring
    FROM opened
    UNION ALL
    SELECT
        o.part_id,
        o.opened_part,
        g.n AS ring_n,
        st_interiorringn(o.part, g.n::integer) AS ring
    FROM opened AS o, generate_series(1, st_numinteriorrings(o.part)) AS g (n)
),

-- one row per ring vertex, without the closing repeat of the first
vertices AS (
    SELECT
        part_id,
        ring_n,
        opened_part,
        len(pts) AS n,
        generate_subscripts(pts, 1) AS i,
        unnest(pts) AS pt
    FROM (
        SELECT
            *,
            list_transform(st_dump(st_points(ring)), d -> d.geom)[1:-2] AS pts
        FROM rings
    )
),

-- renumbered (j, from 0) to start at a vertex near the opened part, so no run wraps
-- around the ring's start; rings with no such vertex are left alone
renumbered AS (
    SELECT
        part_id,
        ring_n,
        pt,
        n,
        off_opened,
        (i - min(i) FILTER (WHERE NOT off_opened) OVER ring + n) % n AS j
    FROM (
        SELECT
            *,
            st_distance(pt, opened_part) > {{ spike_vertex_ft }} AS off_opened
        FROM vertices
    )
    WINDOW ring AS (PARTITION BY part_id, ring_n)
    QUALIFY count(*) FILTER (WHERE NOT off_opened) OVER ring > 0
),

-- a run: a vertex near the opened part and the vertices off it that follow
runs AS (
    SELECT
        *,
        count(*) FILTER (WHERE NOT off_opened) OVER (PARTITION BY part_id, ring_n ORDER BY j) AS run
    FROM renumbered
),

run_ends AS (
    SELECT
        part_id,
        ring_n,
        run,
        list(pt ORDER BY j) AS pts,
        list(j ORDER BY j) AS js,
        (max(j) + 1) % any_value(n) AS next_j
    FROM runs
    GROUP BY part_id, ring_n, run
    HAVING count(*) > 1
),

-- each run's vertices plus the vertex after it
run_paths AS (
    SELECT
        e.part_id,
        e.ring_n,
        e.run,
        list_append(e.pts, nxt.pt) AS pts,
        list_append(e.js, nxt.j) AS js
    FROM run_ends AS e
    INNER JOIN runs AS nxt
        ON e.part_id = nxt.part_id AND e.ring_n = nxt.ring_n AND e.next_j = nxt.j
),

spikes AS (
    SELECT DISTINCT ON (part_id, ring_n, run)
        part_id,
        ring_n,
        js[a + 1:b - 1] AS spike_js
    FROM run_paths, range(1, len(pts) - 1) AS t1 (a), range(a + 2, len(pts) + 1) AS t2 (b)
    WHERE
        st_area(st_makepolygon(st_makeline(list_append(pts[a:b], pts[a]))))
        < {{ spike_half_width_ft }} * st_length(st_makeline(pts[a:b]))
    ORDER BY part_id, ring_n, run, b - a DESC
),

spike_vertices AS (
    SELECT
        part_id,
        ring_n,
        unnest(spike_js) AS j
    FROM spikes
),

-- every ring of each part with a spike, without its spike vertices
spiked_part_rings AS (
    SELECT
        g.part_id,
        g.ring_n,
        coalesce(
            (
                SELECT list(r.pt ORDER BY r.j)
                FROM runs AS r
                WHERE
                    r.part_id = g.part_id
                    AND r.ring_n = g.ring_n
                    AND NOT EXISTS (
                        SELECT 1 FROM spike_vertices AS s
                        WHERE s.part_id = r.part_id AND s.ring_n = r.ring_n AND s.j = r.j
                    )
            ),
            list_transform(st_dump(st_points(g.ring)), d -> d.geom)[1:-2]
        ) AS pts
    FROM rings AS g
    WHERE g.part_id IN (SELECT part_id FROM spikes)
),

-- a ring left with fewer than 3 vertices was all spike: it's dropped, with its part if
-- it was the shell
despiked AS (
    SELECT
        c.precinct,
        st_makepolygon(
            st_makeline(list_append(shell.pts, shell.pts[1])),
            coalesce(
                (
                    SELECT list(st_makeline(list_append(h.pts, h.pts[1])) ORDER BY h.ring_n)
                    FROM spiked_part_rings AS h
                    WHERE h.part_id = c.part_id AND h.ring_n > 0 AND len(h.pts) >= 3
                ),
                []::geometry[]
            )
        ) AS part
    FROM clipped AS c
    INNER JOIN spiked_part_rings AS shell ON c.part_id = shell.part_id AND shell.ring_n = 0
    WHERE len(shell.pts) >= 3
),

despiked_parts AS (
    SELECT
        precinct,
        part
    FROM clipped
    WHERE part_id NOT IN (SELECT part_id FROM spikes)
    UNION ALL
    SELECT
        precinct,
        part
    FROM despiked
),

cleaned AS (
    SELECT
        c.precinct,
        st_makepolygon(
            st_exteriorring(c.part),
            coalesce(
                (
                    SELECT list(st_interiorringn(c.part, g.n::integer))
                    FROM generate_series(1, st_numinteriorrings(c.part)) AS g (n)
                    WHERE st_area(st_makepolygon(st_interiorringn(c.part, g.n::integer))) >= 100
                ),
                []::geometry[]
            )
        ) AS geom
    FROM despiked_parts AS c
    WHERE st_area(c.part) >= 100
)

SELECT
    precinct AS entity_id,
    st_multi(st_union_agg(geom)) AS geom
FROM cleaned
GROUP BY precinct
