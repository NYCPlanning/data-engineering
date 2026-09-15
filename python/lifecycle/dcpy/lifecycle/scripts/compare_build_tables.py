import typer

from dcpy.data import compare
from dcpy.utils import postgres
from dcpy.utils.collections import indented_report

app = typer.Typer()

DEFAULT_PROD_SCHEMA = "nightly_qa"


def comparison_report(
    data_product_database,
    schema_name_dev,
    table_name_dev,
    schema_name_prod,
    table_name_prod,
) -> None:
    client = postgres.PostgresClient(
        database=data_product_database, schema=schema_name_prod
    )
    report = compare.get_sql_report_detailed(
        schema_name_dev=schema_name_dev,
        table_name_dev=table_name_dev,
        schema_name_prod=schema_name_prod,
        table_name_prod=table_name_prod,
        client=client,
    )
    print(indented_report(report.model_dump(), include_line_breaks=True))


@app.command("compare")
def _compare_cli(
    data_product_database: str = typer.Argument(
        help="The product database in the edm-data cluster"
    ),
    schema_name_dev: str = typer.Argument(help="The dev schema"),
    table_name: str = typer.Argument(help="The prod and (if identical) dev table name"),
    schema_name_prod: str = typer.Option(DEFAULT_PROD_SCHEMA, help="The prod schema"),
    table_name_dev: str | None = typer.Option(
        None, help="The dev table name (default: the prod table name)"
    ),
) -> None:
    if not table_name_dev:
        table_name_dev = table_name
    comparison_report(
        data_product_database,
        schema_name_dev,
        table_name_dev,
        schema_name_prod,
        table_name,
    )
