import os
from datetime import datetime

from jinja2 import Template
from sqlalchemy import create_engine, text

# connect to postgres db
engine = create_engine(os.environ["BUILD_ENGINE"])

# Get current year
current_year = datetime.today().year

# SQL Template
sql = """
DROP TABLE IF EXISTS checkbook_spending_by_year;
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
INTO checkbook_spending_by_year
FROM nycoc_checkbook
WHERE TRIM(LEFT(capital_project,12)) in (SELECT DISTINCT maprojid FROM ccp_projects)
GROUP BY TRIM(LEFT(capital_project,12));
"""

# Render template
sql_rendered = Template(sql).render(years=list(range(2010, current_year + 1)))

# Execute SQL
with engine.begin() as conn:
    conn.execute(text(sql_rendered))
