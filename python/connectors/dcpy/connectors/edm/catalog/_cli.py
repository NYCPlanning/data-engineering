import typer
from rich.console import Console
from rich.table import Table

from . import migrate, query

app = typer.Typer(help="Version catalog (DCPY_CATALOG_PG_URL)")


@app.command("migrate")
def _cli_migrate():
    """Create or update the catalog schema. Safe to rerun."""
    applied = migrate()
    print(f"Applied: {', '.join(applied)}" if applied else "Up to date")


@app.command("show")
def _cli_show(
    domain: str = typer.Option("ingest", "--domain", "-d"),
    key: list[str] = typer.Option(None, "--key", "-k", help="Repeatable"),
    store: str = typer.Option(None, "--store", "-s"),
    latest_only: bool = typer.Option(False, "--latest"),
):
    """List catalog rows, e.g. --domain ingest --key dcp_ct2010."""
    rows = query(domain, store=store, keys=key or None, latest_only=latest_only)
    table = Table("store", "key", "version", "latest", "file types", "archived at")
    for r in rows:
        table.add_row(
            r.store,
            r.key,
            r.version,
            "*" if r.is_latest else "",
            ",".join(r.file_types),
            r.archived_at.isoformat() if r.archived_at else "",
        )
    Console().print(table)
    print(f"{len(rows)} row(s)")
