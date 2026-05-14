{% test not_null_if_other_not_null(model, column_name, other_column_name) %}
-- Asserts that whenever `other_column_name` is not null,
-- `column_name` is also not null.
-- Fails on any row where `other_column_name` IS NOT NULL but `column_name` IS NULL.
select *
from {{ model }}
where {{ other_column_name }} is not null
  and {{ column_name }} is null
{% endtest %}
