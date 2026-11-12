{#- 2020 census population by age group as columns, from stg_pff__decennial rows grouped by geography. -#}
{% macro age_group_counts() -%}
{%- set age_groups = {
    'under_18': ['popu18'],
    '18_24': ['age18t19', 'age20', 'age21', 'age22t24'],
    '25_44': ['age25t29', 'age30t34', 'age35t39', 'age40t44'],
    '45_64': ['age45t49', 'age50t54', 'age55t59', 'age60t61', 'age62t64'],
    '65_plus': ['pop65pl'],
} -%}
{%- for group, variables in age_groups.items() %}
    sum(value) FILTER (WHERE census_year = 2020 AND pff_variable IN ('{{ variables | join("', '") }}'))
        AS population_age_{{ group }}_2020_census{{ "," if not loop.last }}
{%- endfor %}
{%- endmacro %}
