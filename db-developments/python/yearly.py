from jinja2 import Template
from datetime import datetime
from pathlib import Path
import os
import sys

if __name__ == "__main__":
    filename = sys.argv[1]
    decade = sys.argv[2]  # e.g. "10" or "20"
    CAPTURE_DATE = os.environ.get("CAPTURE_DATE")

    # Get current year
    current_year = datetime.today().year

    # SQL Template
    with open(filename, 'r') as f:
        sql = f.read()

    # Render template
    sql_rendered = Template(sql).render(
        years=list(range(2010, current_year+1)),
        decade=decade,
        CAPTURE_DATE=CAPTURE_DATE)

    print(sql_rendered)
