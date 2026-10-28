"""qa__rpl_order checks RPL.txt's row order, so its order must be the export's."""

from pathlib import Path

import yaml

PRODUCT = Path(__file__).parent.parent


def test_qa_rpl_order_uses_the_export_order_by():
    recipe = yaml.safe_load((PRODUCT / "recipe.yml").read_text())
    (rpl,) = [d for d in recipe["exports"]["datasets"] if d["name"] == "rpl"]
    project = yaml.safe_load((PRODUCT / "dbt_project.yml").read_text())
    assert rpl["custom"]["order_by"] == project["vars"]["rpl_file_order_by"]
