"""Alembic environment. Migrations are raw SQL (no SQLAlchemy models): online mode only."""

from alembic import context
from sqlalchemy import create_engine, pool

from dispatch.db import sqlalchemy_url

url = context.config.get_main_option("sqlalchemy.url")
if not url:
    raise RuntimeError("dispatch migrations need sqlalchemy.url (see dispatch.db.migrate)")

engine = create_engine(sqlalchemy_url(url), poolclass=pool.NullPool)
with engine.connect() as connection:
    context.configure(connection=connection, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()
engine.dispose()
