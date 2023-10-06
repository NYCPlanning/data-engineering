from dcpy.builds import environment
from dcpy.builds import load

from . import PG_CLIENT, RECIPE_PATH


if __name__ == "__main__":
    environment.setup(PG_CLIENT)
    load.load_source_data(
        PG_CLIENT,
        RECIPE_PATH,
    )
