{#
    Fisher's exact natural-breaks classification, emitted as a chain of CTEs.

    `relation` must expose (geo, range, units). The chain ends with a CTE named
    `output_cte` holding one row per (geo, range, class_id) with range_min and
    range_max, class_id 1..k ascending. Call it inside a WITH list and put the
    comma after it:

        WITH measures AS (...),
        {{ jenks_break_ctes('measures', 'breaks') }},
        next_thing AS (...)

    Reproduces R's classInt::classify_intervals(x, k, 'jenks'), which the old
    cpp.R used: only positive values are classified, and class 1 starts at 0
    rather than at the smallest value.

    Fisher's algorithm is a dynamic program, cost[m][j] = min over a of
    cost[m-1][a-1] + ssd(a, j). The natural way to write that is a recursive
    CTE, but Postgres forbids aggregates and ORDER BY in a recursive term,
    which is exactly what the argmin needs. k is a small constant, so the
    levels are unrolled into k CTEs instead. Prefix sums make each ssd() O(1),
    leaving O(k * n^2) row pairs: at n = 262 that is a few hundred thousand.

    Groups with fewer than k positive values are dropped, since there is no
    k-way split of them.
#}

{% macro jenks_break_ctes(relation, output_cte, k=5) %}
_jenks_positive AS (
    SELECT geo, "range", units::double precision AS v
    FROM {{ relation }}
    WHERE units > 0
),

_jenks_sized AS (
    SELECT geo, "range"
    FROM _jenks_positive
    GROUP BY geo, "range"
    HAVING count(*) >= {{ k }}
),

_jenks_vals AS (
    SELECT
        p.geo,
        p."range",
        p.v,
        row_number() OVER (PARTITION BY p.geo, p."range" ORDER BY p.v) AS i
    FROM _jenks_positive AS p
    INNER JOIN _jenks_sized AS s ON p.geo = s.geo AND p."range" = s."range"
),

_jenks_pre AS (
    SELECT
        geo,
        "range",
        i,
        v,
        sum(v) OVER (PARTITION BY geo, "range" ORDER BY i) AS s1,
        sum(v * v) OVER (PARTITION BY geo, "range" ORDER BY i) AS s2
    FROM _jenks_vals
),

-- index 0 sentinel, so a class starting at the first value has a prefix to subtract
_jenks_pre0 AS (
    SELECT DISTINCT
        geo,
        "range",
        0 AS i,
        0::double precision AS s1,
        0::double precision AS s2
    FROM _jenks_pre
    UNION ALL
    SELECT geo, "range", i, s1, s2
    FROM _jenks_pre
),

_jenks_sizes AS (
    SELECT geo, "range", max(i) AS n
    FROM _jenks_pre
    GROUP BY geo, "range"
),

_jenks_lvl1 AS (
    SELECT
        geo,
        "range",
        i AS j,
        s2 - s1 * s1 / i AS cost,
        ARRAY[]::int[] AS brk
    FROM _jenks_pre
),
{% for m in range(2, k + 1) %}
_jenks_lvl{{ m }} AS (
    SELECT DISTINCT ON (p.geo, p."range", p.i)
        p.geo,
        p."range",
        p.i AS j,
        l.cost
        + (p.s2 - pa.s2)
        - (p.s1 - pa.s1) * (p.s1 - pa.s1) / (p.i - l.j) AS cost,
        l.brk || l.j AS brk
    FROM _jenks_pre AS p
    INNER JOIN _jenks_lvl{{ m - 1 }} AS l
        ON
            p.geo = l.geo
            AND p."range" = l."range"
            AND l.j < p.i
            AND l.j >= {{ m - 1 }}
    INNER JOIN _jenks_pre0 AS pa
        ON p.geo = pa.geo AND p."range" = pa."range" AND pa.i = l.j
    ORDER BY p.geo ASC, p."range" ASC, p.i ASC, cost ASC
),
{% endfor %}
_jenks_solution AS (
    SELECT
        l.geo,
        l."range",
        l.brk || l.j AS cut_idx
    FROM _jenks_lvl{{ k }} AS l
    INNER JOIN _jenks_sizes AS z
        ON l.geo = z.geo AND l."range" = z."range" AND l.j = z.n
),

_jenks_cuts AS (
    SELECT
        s.geo,
        s."range",
        c.ord::int AS class_id,
        p.v AS range_max
    FROM _jenks_solution AS s
    CROSS JOIN LATERAL unnest(s.cut_idx) WITH ORDINALITY AS c (idx, ord)
    INNER JOIN _jenks_pre AS p
        ON s.geo = p.geo AND s."range" = p."range" AND p.i = c.idx
),

{{ output_cte }} AS (
    SELECT
        geo,
        "range",
        class_id,
        range_max,
        coalesce(
            lag(range_max) OVER (PARTITION BY geo, "range" ORDER BY class_id), 0
        ) AS range_min
    FROM _jenks_cuts
)
{% endmacro %}
