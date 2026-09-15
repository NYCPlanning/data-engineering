{#
    The latest calendar year fully covered by the pinned dcp_housing version.

    Wraps the cpp_latest_complete_year var so the CPP models still render
    parseable SQL when CI lints them. CI runs sqlfluff with --templater=jinja,
    whose var() stub always returns empty and ignores any default, which turned
    the year into 0 and the column names into completed_units_-9_0. The dbt
    templater resolves var() properly but needs a live database connection,
    which a lint job should not.

    So: take the var when dbt is running it, and otherwise fall back to the
    plain jinja variable set in this product's .sqlfluff. Keep the two in step.
#}

{% macro cpp_latest_year() -%}
{{ (var('cpp_latest_complete_year', '') | int) or (cpp_latest_complete_year | default(2025) | int) }}
{%- endmacro %}
