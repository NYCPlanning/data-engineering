from jinja2 import Template
from datetime import datetime
from sqlalchemy import create_engine
import os

# connect to postgres db
engine = create_engine(os.environ.get("BUILD_ENGINE", ""))

# Get current year
current_year = datetime.today().year

# SQL Template
sql = """
DROP TABLE IF EXISTS cpdb_projects_spending_byyear;
SELECT 
    TRIM(LEFT(capital_project,12)) AS maprojid,
    {%- for year in years %}
    SUM(
        CASE WHEN EXTRACT(year FROM issue_date::date) = '{{ year }}' 
        THEN check_amount::double precision END
    ) AS spend{{ year }}
    {%- if not loop.last -%}
        , 
    {%- endif -%}
    {% endfor %}
INTO cpdb_projects_spending_byyear
FROM capital_spending
WHERE TRIM(LEFT(capital_project,12)) in (SELECT DISTINCT maprojid FROM cpdb_projects)
GROUP BY TRIM(LEFT(capital_project,12));
"""

# Render template
sql_rendered = Template(sql).render(years=list(range(2010, current_year+1)))

# Execute SQL
engine.execute(sql_rendered)
