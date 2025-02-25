from pathlib import Path
import shutil

from dcpy.lifecycle.builds import plan, load

RESOURCES = Path(__file__).parent / "resources"
RECIPE_PATH = RESOURCES / "recipe.yml"

assert RESOURCES.exists()

def test_resolving_and_loading_recipes(tmp_path):
    temp_dir = Path(tmp_path)
    temp_recipe_path = temp_dir / "recipe.yml"

    shutil.copyfile(RECIPE_PATH, temp_recipe_path)
    lock_path = plan.plan(temp_recipe_path)
    # versions = [d.version for d in planned.inputs.datasets]
    # assert "latest" not in versions, "All recipe versions should have been resolved"

    load.load_source_data(lock_path)
