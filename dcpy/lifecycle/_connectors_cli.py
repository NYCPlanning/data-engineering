import typer

from dcpy.lifecycle.connector_registry import connectors

app = typer.Typer()


@app.command("get_versions")
def _cli_wrapper_get_versions(
    connector: str = typer.Argument(
        help="Connector Name",
    ),
    key: str = typer.Argument(
        help="Resource Key",
    ),
    # TODO: support for kwargs / opts. (e.g. exclude_latest for publishing)
):
    conn = connectors[connector]
    print(conn.list_versions(key))
