from dcpy.utils import postgres


def setup(pg_client: postgres.PostgresClient):
    pg_client.drop_schema()
    pg_client.create_schema()
    # TODO delete draft S3 folder
