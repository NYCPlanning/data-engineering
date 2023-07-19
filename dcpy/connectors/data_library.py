from dcpy.connectors.edm import recipes
from dcpy import DCPY_ROOT_PATH
from dcpy.connectors import psql
from dcpy import BUILD_ENGINE_RAW, build_engine
from sqlalchemy import text, update, insert
from sqlalchemy.schema import Table, MetaData

LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"


def create_source_data_table():
    """Create the source_data_versions table"""
    with build_engine.begin() as con:
        con.execute(
            text(
                """DROP TABLE IF EXISTS source_data_versions;
                CREATE TABLE source_data_versions (
                schema_name character varying,
                v character varying);"""
            )
        )


def import_recipe(
    recipe_name: str,
    *,
    version="latest",
    local_library_dir=LIBRARY_DEFAULT_PATH,
    set_version=False,
):
    """
    Imports a recipe to local data library folder and build engine,
    and adds versioning info to the imported table.
    """
    config = recipes.get_config(recipe_name, version)
    recipe_version = config["dataset"]["version"]
    sql_script_path = recipes.fetch_sql(recipe_name, recipe_version, local_library_dir)

    psql.exec_file_via_shell(BUILD_ENGINE_RAW, sql_script_path)

    with build_engine.connect() as con:
        con.execute(
            text(f"ALTER TABLE {recipe_name} ADD COLUMN data_library_version text;")
        )
        con.commit()

        recipes_table = Table(recipe_name, MetaData(), autoload_with=build_engine)
        con.execute(update(recipes_table).values(data_library_version=version))

        if set_version:
            data_version_table = Table(
                "source_data_versions", MetaData(), autoload_with=build_engine
            )
            con.execute(
                insert(data_version_table).values(schema_name=recipe_name, v=version)
            )

        con.commit()
