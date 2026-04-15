from dagster import Definitions, asset


@asset
def hello_world():
    """A simple hello world asset."""
    return "Hello from Dagster!"


defs = Definitions(
    assets=[hello_world],
)
