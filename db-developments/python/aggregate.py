from jinja2 import Template
from datetime import datetime
from pathlib import Path
import os
import sys
from sqlalchemy import text
from utils import engine

geoms = {
    "block": {
        "source_column": "bctcb{{decade}}",
        "output_column": "bctcb{{decade}}",
        "group_by": ["boro", "bctcb{{decade}}", "cenblock{{decade}}"],
        "join_table": "dcp_cb{{decade}}"
    },
    "cdta": {
        "source_column": "cdta{{decade}}",
        "output_column": "cdta{{decade}}",
        "join_table": "dcp_cdta{{decade}}"
    },
    "commntydst": {
        "source_column": "comunitydist",
        "join_table": "dcp_cdboundaries"
    },
    "councildst": {
        "source_column": "councildist",
        "join_table": "dcp_councildistricts"
    },
    "nta": {
        "source_column": "nta{{ decade }}",
        "output_column": "nta{{ decade }}",
        "join_table": "dcp_nta{{decade}}"
    },
    "tract": {
        "source_column": "bct{{decade}}",
        "output_column": "bct{{decade}}",
        "group_by": ["boro", "bct{{decade}}", "centract{{decade}}"],
        "join_table": "dcp_ct{{decade}}"
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
        print(f"hello! geom is {geom}")
    except:
        print("No geom supplied, assuming yearly aggregation")
        sql_rendered = Template(sql).render(
            years=list(range(2010, current_year+1)),
            decade=decade,
            CAPTURE_DATE=CAPTURE_DATE)
    else:
        print(f"geom of {geom} supplied, aggregating by {geom}")
        column_info = geoms[geom]
        source_column = column_info.get("source_column", geom)
        output_column = column_info.get("output_column", geom)
        group_by = column_info.get("group_by", [output_column])
        join_table = column_info["join_table"]

        # Render template in twice because decade is part of render inputs as well
        sql_rendered = Template(sql).render(
            years=list(range(2010, current_year+1)),
            CAPTURE_DATE=CAPTURE_DATE,
            decade=decade,
            geom=geom,
            source_column=source_column,
            output_column=output_column,
            group_by=group_by,
            join_table=join_table)
        
        sql_rendered = Template(sql_rendered).render(decade=decade)

    with engine.begin() as connection:
        connection.execute(text(sql_rendered))
