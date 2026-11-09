{#- Median household income and households by income group as columns, from stg_pff__acs rows grouped by geography. -#}
{% macro household_income_metrics() -%}
{%- set income_groups = {
    'under_25k': ['hhiu10', 'hhi10t14', 'hhi15t24'],
    '25k_50k': ['hhi25t34', 'hhi35t49'],
    '50k_100k': ['hhi50t74', 'hhi75t99'],
    '100k_200k': ['hi100t149', 'hi150t199'],
    '200k_plus': ['hhi200pl'],
} %}
    max(estimate) FILTER (WHERE pff_variable = 'mdhhinc') AS median_household_income_acs,
    max(margin_of_error) FILTER (WHERE pff_variable = 'mdhhinc') AS median_household_income_acs_moe,
    max(estimate) FILTER (WHERE pff_variable = 'hh2') AS households_acs,
{%- for group, variables in income_groups.items() %}
    sum(estimate) FILTER (WHERE pff_variable IN ('{{ variables | join("', '") }}')) AS households_income_{{ group }}_acs{{ "," if not loop.last }}
{%- endfor %}
{%- endmacro %}
