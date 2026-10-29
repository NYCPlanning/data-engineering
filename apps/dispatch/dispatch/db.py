"""Connections and migrations.

psycopg 3 for all queue traffic. Alembic (which needs SQLAlchemy) only runs migrations.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def sqlalchemy_url(url: str) -> str:
    """Point a plain postgres URL at SQLAlchemy's psycopg 3 dialect."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix) :]
    return url


def libpq_url(url: str) -> str:
    """Strip a SQLAlchemy driver suffix so psycopg can use the URL."""
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def alembic_config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    # ConfigParser interpolation: escape % in passwords
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def migrate(url: str) -> None:
    command.upgrade(alembic_config(url), "head")


def head_revision() -> str | None:
    return ScriptDirectory(str(MIGRATIONS_DIR)).get_current_head()


def current_revision(conn: psycopg.Connection[DictRow]) -> str | None:
    row = conn.execute(
        "SELECT version_num FROM alembic_version LIMIT 1", prepare=False
    ).fetchone()
    if row is None:
        return None
    return row["version_num"]


def connect(url: str) -> psycopg.Connection[DictRow]:
    # prepare_threshold=None: no server-side prepared statements, so this works behind
    # a transaction-mode PgBouncer (Azure Flexible Server's built-in one).
    return psycopg.Connection.connect(
        libpq_url(url), autocommit=True, row_factory=dict_row, prepare_threshold=None
    )


def make_pool(
    url: str, min_size: int = 1, max_size: int = 10
) -> ConnectionPool[psycopg.Connection[DictRow]]:
    return ConnectionPool(
        libpq_url(url),
        connection_class=psycopg.Connection[DictRow],
        min_size=min_size,
        max_size=max_size,
        kwargs={"autocommit": True, "row_factory": dict_row, "prepare_threshold": None},
        open=True,
    )
