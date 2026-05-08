{% test many_to_one(model, column_name, parent_column) %}
-- Asserts that each value of `column_name` (the "many" side) is associated with
-- exactly one value of `parent_column` (the "one" side).
-- Fails if any `column_name` value maps to more than one `parent_column` value,
-- which would indicate a many-to-many relationship.
select
    {{ column_name }},
    count(distinct {{ parent_column }}) as parent_count
from {{ model }}
group by {{ column_name }}
having count(distinct {{ parent_column }}) > 1
{% endtest %}
