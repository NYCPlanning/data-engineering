/* 
This macro can be used with dbt models, source tables, and CTEs.
The geometry column in the output is named "geom"

For CTEs, must include "left_columns" parameter. The CTE name should be in quotes

If you include a geometry column in "left_columns" parameter, then you will have both original geom column 
from "left" table and the resulting geom column named "geom"
*/

{% macro nullif_by_jobtype(jobtype_clause, field, nullif, cast="") %}
    CASE
        WHEN jobtype {{ jobtype_clause }} THEN nullif({{ field }}, {{ nullif }})
        ELSE {{ field }}
    END {{ cast }} AS {{ field }}

{% endmacro %}
