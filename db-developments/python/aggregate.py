from jinja2 import Template
from datetime import datetime
import os
import sys
from sqlalchemy import text
from utils import engine

geoms = {
    "block": {
        "source_column": "bctcb{{decade}}",
        "output_column": "bctcb{{decade}}",
        "additional_column_mappings": [
            ("geoid", "cenblock{{decade[:-2]}}")
        ],
        "group_by": ["boro", "bctcb{{decade}}", "cenblock{{decade}}"],
        "join_table": "dcp_cb{{decade}}"
    },
    "cdta": {
        "source_column": "cdta{{decade}}",
        "output_column": "cdta{{decade}}",
        "additional_column_mappings": [
            ("cdtaname", "cdtaname{{decade[:-2]}}")
        ],
        "join_table": "dcp_cdta{{decade}}"
    },
    "commntydst": {
        "source_column": "comunitydist",
        "join_table": "dcp_cdboundaries",
        "right_join_column": "borocd::TEXT"
    },
    "councildst": {
        "source_column": "councildist",
        "join_table": "dcp_councildistricts",
        "right_join_column": "coundist::TEXT"
    },
    "nta": {
        "source_column": "nta{{decade}}",
        "output_column": "nta{{decade}}",
        "additional_column_mappings": [
            ("ntaname", "ntaname{{decade[:-2]}}")
        ],
        "join_table": "dcp_nta{{decade}}"
    },
    "tract": {
        "source_column": "bct{{decade}}",
        "output_column": "bct{{decade}}",
        "additional_column_mappings": [
            ("geoid", "centract{{decade}}")
        ],
        "group_by": ["boro", "bct{{decade}}", "centract{{decade}}"],
        "join_table": "dcp_ct{{decade}}",
        "right_join_column": "boroct2020"
    }
}

if __name__ == "__main__":
    filename = sys.argv[1]
    decade = sys.argv[2]  # e.g. "10" or "20"
    CAPTURE_DATE = os.environ.get("CAPTURE_DATE")

    # Get current year
    current_year = datetime.today().year

    # SQL Template
    with open(filename, 'r') as f:
        sql = f.read()

    try:
        geom=sys.argv[3]
    except IndexError:
        print(f"Aggregating by year for decade {decade}")
        sql_rendered = Template(sql).render(
            years=list(range(2010, current_year+1)),
            decade=decade,
            CAPTURE_DATE=CAPTURE_DATE)
    else:
        print(f"Aggregating by {geom}")
        column_info = geoms[geom]
        source_column = column_info.get("source_column", geom)
        output_column = column_info.get("output_column", geom)
        output_column_internal = column_info.get("output_column_internal", output_column)
        additional_column_mappings = column_info.get("additional_column_mappings", [])
        group_by = column_info.get("group_by", [output_column])
        join_table = column_info["join_table"]
        right_join_column = column_info.get("right_join_column", output_column)

        # Render template in twice because decade is part of render inputs as well
        sql_rendered = Template(sql).render(
            years=list(range(2010, current_year+1)),
            CAPTURE_DATE=CAPTURE_DATE,
            decade=decade,
            geom=geom,
            source_column=source_column,
            output_column=output_column,
            output_column_internal=output_column_internal,
            additional_column_mappings=additional_column_mappings,
            group_by=group_by,
            join_table=join_table,
            right_join_column=right_join_column)
        
        sql_rendered = Template(sql_rendered).render(decade=decade)
    
    with engine.begin() as connection:
        connection.execute(text(sql_rendered))
