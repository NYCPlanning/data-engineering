{#- KPDB's longform columns, the same for every geography apart from the suffix on the _in_<geography> ones. -#}
{% macro kpdb_record_columns(suffix) -%}
    project_id,
    source,
    record_name,
    status,
    type,
    date AS record_date,
    date_type,
    classb::integer = 1 AS is_classb,
    nycha::integer = 1 AS is_nycha,
    senior_housing::integer = 1 AS is_senior_housing,
    inactive::integer = 1 AS is_inactive,
    proportion_in_{{ suffix }}::double AS proportion_in_{{ suffix }},
    units_net_in_{{ suffix }}::double AS units_net_in_{{ suffix }},
    completed_units_in_{{ suffix }}::double AS completed_units_in_{{ suffix }},
    future_phased_units_total_in_{{ suffix }}::double AS future_phased_units_total_in_{{ suffix }},
    future_units_without_phasing_in_{{ suffix }}::double AS future_units_without_phasing_in_{{ suffix }},
    within_5_years_in_{{ suffix }}::double AS within_5_years_in_{{ suffix }},
    from_5_to_10_years_in_{{ suffix }}::double AS from_5_to_10_years_in_{{ suffix }},
    after_10_years_in_{{ suffix }}::double AS after_10_years_in_{{ suffix }}
{%- endmacro %}
