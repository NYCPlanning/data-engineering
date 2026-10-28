"""generate_docs: the Record order section's table, from lion_outputs.csv."""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import generate_docs as gd  # noqa: E402


def test_borough_files_collapse_to_a_pattern():
    names = [f"{b}LION.dat" for b in gd._BOROUGH_FILE_PREFIXES]
    assert gd._file_names_cell(names) == "{Borough}LION.dat"
    assert gd._file_names_cell(["GenericD.txt", "RoadbedD.txt"]) == (
        "GenericD.txt, RoadbedD.txt"
    )


def test_every_prod_order_file_is_in_the_table():
    with open(gd.LION_OUTPUTS_PATH, newline="") as f:
        prod_order = [r for r in csv.DictReader(f) if r["order_rule"] == "prod"]
    table = gd._record_order_table()
    cells = " | ".join(row[0] for row in table.rows)
    for row in prod_order:
        name = row["filename"]
        for prefix in gd._BOROUGH_FILE_PREFIXES:
            if name.startswith(prefix) and name != "nyc.thinlion":
                name = "{Borough}" + name[len(prefix) :]
        assert name in cells, row["filename"]
    # where prod departs from the spec comes first, with spec lines spelled out
    assert table.rows[0][3] == "Yes"
    assert not any("L5" in row[2] for row in table.rows)
