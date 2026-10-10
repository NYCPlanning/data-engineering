"""Chain-shaped connected-components stress test for the point_to_node recursion, in
DuckDB (UNION closure vs USING KEY label propagation) and PG (UNION closure, the dbt
model's formulation). Real merge clusters are 2-3 cells, so the build itself never
recurses more than twice; this shows how each engine scales if that ever changes.

    python stress.py 500 1000 2000
"""

import os
import sys
import time

import duckdb
import psycopg2

MERGES = """
    SELECT i AS cell_a, i + 1 AS cell_b FROM generate_series(1, {n} - 1) AS g (i)
    UNION ALL
    SELECT i + 1, i FROM generate_series(1, {n} - 1) AS g (i)
"""

CLOSURE = """
    WITH RECURSIVE cell_merges AS MATERIALIZED ({merges}),
    reachable (start_cell, reached_cell) AS (
        SELECT DISTINCT cell_a, cell_a FROM cell_merges
        UNION
        SELECT r.start_cell, m.cell_b
        FROM reachable AS r INNER JOIN cell_merges AS m ON r.reached_cell = m.cell_a
    )
    SELECT count(DISTINCT c) FROM (
        SELECT min(reached_cell) AS c FROM reachable GROUP BY start_cell
    ) AS x
"""

USING_KEY = """
    WITH RECURSIVE cell_merges AS MATERIALIZED ({merges}),
    labels (cell, comp) USING KEY (cell) AS (
        SELECT DISTINCT cell_a, cell_a FROM cell_merges
        UNION ALL
        SELECT m.cell_b, min(l.comp)
        FROM labels AS l
        INNER JOIN cell_merges AS m ON l.cell = m.cell_a
        INNER JOIN recurring.labels AS cur ON m.cell_b = cur.cell
        GROUP BY m.cell_b
        HAVING min(l.comp) < any_value(cur.comp)
    )
    SELECT count(DISTINCT comp) FROM labels
"""

PG_MAX_N = 2000


def timed(fn):
    t = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - t


def main() -> None:
    sizes = [int(n) for n in sys.argv[1:]] or [500, 1000, 2000]
    duck = duckdb.connect()
    pg = psycopg2.connect(f"{os.environ['BUILD_ENGINE_SERVER']}/db-cscl")
    pg.autocommit = True
    cur = pg.cursor()
    cur.execute("SET statement_timeout = '5min'")
    print(
        f"{'n':>6s} {'duck closure':>13s} {'duck using key':>15s} {'pg closure':>11s}"
    )
    for n in sizes:
        merges = MERGES.format(n=n)
        (r1,), t1 = timed(lambda: duck.sql(CLOSURE.format(merges=merges)).fetchone())
        (r2,), t2 = timed(lambda: duck.sql(USING_KEY.format(merges=merges)).fetchone())

        def run_pg():
            cur.execute(CLOSURE.format(merges=merges))
            return cur.fetchone()

        # PG's closure is ~quadratic and already ~14s at n=2000
        if n <= PG_MAX_N:
            (r3,), t3 = timed(run_pg)
            pg_time = f"{t3:10.2f}s"
        else:
            r3, pg_time = 1, f"{'skipped':>11s}"
        assert r1 == r2 == r3 == 1, (r1, r2, r3)
        print(f"{n:6d} {t1:12.2f}s {t2:14.2f}s {pg_time}", flush=True)
    pg.close()


if __name__ == "__main__":
    main()
