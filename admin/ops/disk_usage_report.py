"""
Disk usage diagnostic across all build databases.

Surfaces:
  - Database sizes (cluster-level)
  - Inactive replication slots holding WAL
  - Per-database: schema sizes, top tables, bloated tables, unused indexes

Usage:
    python3 admin/ops/disk_usage_report.py [--db DB_NAME] [--output PATH]

With --db, report only the named database (useful for drilling in). The report is
both printed and saved to a file (default: .lifecycle/logs/disk_usage_<timestamp>.txt).
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import typer
from sqlalchemy import create_engine, text

from dcpy.connectors.edm.connectors import BUILD_DBS
from dcpy.utils import postgres

IGNORED_SCHEMAS = frozenset(
    postgres.PROTECTED_POSTGRES_SCHEMAS
    + ["pg_toast", "tiger", "tiger_data", "topology"]
)


class _Tee:
    """Writes to both stdout and a file simultaneously."""

    def __init__(self, path: Path):
        self._file = path.open("w")
        self._stdout = sys.stdout

    def write(self, text: str):
        self._stdout.write(text)
        self._file.write(text)

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        self._file.close()


pd.set_option("display.max_rows", 100)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.width", 120)

app = typer.Typer(add_completion=False)


def _engine(database: str):
    server = os.environ["BUILD_ENGINE_SERVER"]
    uri = postgres.generate_engine_uri(server, database)
    return create_engine(uri, isolation_level="AUTOCOMMIT")


def _query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def _section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _print(df: pd.DataFrame, indent: int = 2):
    prefix = " " * indent
    for line in df.to_string(index=False).splitlines():
        print(prefix + line)


# ---------------------------------------------------------------------------
# Cluster-level queries (can run from any database)
# ---------------------------------------------------------------------------


def report_database_sizes(engine):
    _section("Database sizes (cluster)")
    df = _query(
        engine,
        """
        SELECT
            datname                                     AS database,
            pg_size_pretty(pg_database_size(datname))   AS size,
            pg_database_size(datname)                   AS size_bytes
        FROM pg_database
        WHERE datname NOT IN ('template0', 'template1', 'postgres', 'defaultdb', 'rdsadmin')
        ORDER BY pg_database_size(datname) DESC
    """,
    )
    df = df.drop(columns=["size_bytes"])
    _print(df)


def report_replication_slots(engine):
    _section("Replication slots (WAL retention risk)")
    try:
        df = _query(
            engine,
            """
            SELECT
                slot_name,
                slot_type,
                active,
                pg_size_pretty(
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                ) AS wal_retained
            FROM pg_replication_slots
        """,
        )
    except Exception:
        # pg_wal_lsn_diff may be restricted on managed databases; fall back
        try:
            df = _query(
                engine,
                """
                SELECT slot_name, slot_type, active
                FROM pg_replication_slots
            """,
            )
        except Exception as e:
            print(f"  (skipped — insufficient privileges: {e})")
            return
    if df.empty:
        print("  (none)")
    else:
        _print(df)
        inactive = df[~df["active"]]
        if not inactive.empty:
            print(
                "\n  !! Inactive slots above are holding WAL — consider dropping them:"
            )
            for name in inactive["slot_name"]:
                print(f"     SELECT pg_drop_replication_slot('{name}');")


# ---------------------------------------------------------------------------
# Per-database queries
# ---------------------------------------------------------------------------


def report_schema_sizes(engine, database: str):
    _section(f"[{database}] Schema sizes")
    df = _query(
        engine,
        """
        SELECT
            n.nspname                                                   AS schema,
            pg_size_pretty(sum(pg_total_relation_size(c.oid))::bigint)  AS total_size,
            sum(pg_total_relation_size(c.oid))                          AS total_bytes,
            count(*)                                                    AS relations
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm')
        GROUP BY n.nspname
        ORDER BY sum(pg_total_relation_size(c.oid)) DESC
    """,
    )
    df = df.drop(columns=["total_bytes"])
    _print(df)


def report_top_tables(engine, database: str, limit: int = 20):
    _section(f"[{database}] Top {limit} tables by total size")
    df = _query(
        engine,
        f"""
        SELECT
            schemaname                                                      AS schema,
            tablename                                                       AS table,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename))       AS heap,
            pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename))        AS indexes,
            pg_size_pretty(
                pg_total_relation_size(schemaname||'.'||tablename)
                - pg_relation_size(schemaname||'.'||tablename)
                - pg_indexes_size(schemaname||'.'||tablename)
            )                                                               AS toast
        FROM pg_tables
        WHERE schemaname NOT IN ({", ".join(repr(s) for s in IGNORED_SCHEMAS)})
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT {limit}
    """,
    )
    _print(df)


def report_bloat(engine, database: str):
    _section(f"[{database}] Table bloat (dead tuples > 10%)")
    df = _query(
        engine,
        """
        SELECT
            schemaname                                                          AS schema,
            relname                                                             AS table,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname))   AS total_size,
            n_live_tup                                                          AS live,
            n_dead_tup                                                          AS dead,
            round(
                n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 1
            )                                                                   AS dead_pct,
            coalesce(last_vacuum::date::text, last_autovacuum::date::text, 'never') AS last_vacuum
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 1000
          AND round(
                n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 1
              ) > 10
        ORDER BY n_dead_tup DESC
        LIMIT 20
    """,
    )
    if df.empty:
        print("  (no bloated tables found)")
    else:
        _print(df)
        print("\n  To reclaim space (locks table; run during downtime):")
        print("     VACUUM FULL ANALYZE <schema>.<table>;")


def report_unused_indexes(engine, database: str, min_size_mb: int = 1):
    _section(f"[{database}] Unused indexes >= {min_size_mb} MB (idx_scan = 0)")
    df = _query(
        engine,
        f"""
        SELECT
            schemaname                                          AS schema,
            relname                                             AS table,
            indexrelname                                        AS index,
            pg_size_pretty(pg_relation_size(indexrelid))        AS index_size,
            pg_relation_size(indexrelid)                        AS index_bytes
        FROM pg_stat_user_indexes
        WHERE idx_scan = 0
          AND pg_relation_size(indexrelid) >= {min_size_mb * 1024 * 1024}
        ORDER BY pg_relation_size(indexrelid) DESC
        LIMIT 30
    """,
    )
    if df.empty:
        print(f"  (no unused indexes >= {min_size_mb} MB)")
    else:
        total_mb = df["index_bytes"].sum() / (1024 * 1024)
        df = df.drop(columns=["index_bytes"])
        _print(df)
        print(f"\n  Total reclaimable from unused indexes: {total_mb:.0f} MB")
        print("  To drop (verify first that the index is truly unused):")
        print("     DROP INDEX CONCURRENTLY <schema>.<index>;")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


@app.command()
def main(
    db: str = typer.Option(None, "--db", help="Limit report to a single database"),
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Write report to this file (default: disk_usage_<timestamp>.txt)",
    ),
):
    """Print disk usage diagnostics across build databases."""
    out_path = (
        output
        or Path(".lifecycle/logs")
        / f"disk_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    databases = [db] if db else BUILD_DBS

    original_stdout = sys.stdout
    tee = _Tee(out_path)
    sys.stdout = tee
    try:
        # Use the first listed db for cluster-level queries
        cluster_engine = _engine(databases[0])
        report_database_sizes(cluster_engine)
        report_replication_slots(cluster_engine)
        cluster_engine.dispose()

        for database in databases:
            print(f"\n{'#' * 60}")
            print(f"#  {database}")
            print(f"{'#' * 60}")
            try:
                engine = _engine(database)
                report_schema_sizes(engine, database)
                report_top_tables(engine, database)
                report_bloat(engine, database)
                report_unused_indexes(engine, database)
                engine.dispose()
            except Exception as e:
                print(f"  ERROR connecting to {database}: {e}")
    finally:
        sys.stdout = original_stdout
        tee.close()

    print(f"\nReport saved to {out_path}")


if __name__ == "__main__":
    app()
