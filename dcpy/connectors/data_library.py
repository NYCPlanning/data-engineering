from dcpy.connectors.edm import recipes
from dcpy import DCPY_ROOT_PATH
from dcpy.connectors import psql
from dcpy import BUILD_ENGINE_RAW, build_engine
from sqlalchemy import text

LIBRARY_DEFAULT_PATH = DCPY_ROOT_PATH.parent / ".library"


def import_recipe(
    name: str,
    *,
    version="latest",
    local_library_dir=LIBRARY_DEFAULT_PATH,
    set_version=False,
):
    """
    Imports a recipe to local data library folder and build engine,
    and adds versioning info to the imported table.
    """
    config = recipes.get_config(name, version)
    recipe_version = config["dataset"]["version"]
    sql_script_path = recipes.fetch_sql(name, recipe_version, local_library_dir)

    psql.exec_file_via_shell(BUILD_ENGINE_RAW, sql_script_path)

    with build_engine.begin() as con:
        con.execute(
            text(
                f"ALTER TABLE {name} ADD COLUMN data_library_version text; \
                UPDATE {name} SET data_library_version = '{version}';"
            )
        )

        if set_version:
            con.execute(
                text(
                    f"INSERT INTO source_data_versions VALUES ('{name}','{version}');",
                )
            )
