{#-
People below the federal poverty level, families with income under NYC's True Cost of Living threshold for families
with children and adults under 65, and the people in families that measure covers, as columns, from stg_pff__acs rows grouped by geography. Counts, so
they add up across geographies.
-#}
{% macro poverty_metrics() -%}
{%- set brackets_under_150k = [
    'famiu10', 'fami10t14', 'fami15t24', 'fami25t34', 'fami35t49', 'fami50t74', 'fami75t99', 'fi100t149'
] %}
    sum(estimate) FILTER (WHERE pff_variable = 'pbwpv') AS people_below_poverty_acs,
    max(margin_of_error) FILTER (WHERE pff_variable = 'pbwpv') AS people_below_poverty_acs_moe,
    sum(estimate) FILTER (WHERE pff_variable = 'poppvu1') AS people_poverty_status_determined_acs,
    sum(estimate) FILTER (WHERE pff_variable = 'fam2') AS families_acs,
    sum(estimate) FILTER (WHERE pff_variable = 'pop_1') AS population_acs,
    -- PFF's base for average family size (avgfmsz), people in family households
    sum(estimate) FILTER (WHERE pff_variable = 'popinfms') AS people_in_families_acs,
    -- the threshold falls inside the $150,000-$199,999 bracket, so the share of that bracket under it is counted,
    -- assuming incomes are spread evenly across the bracket
    sum(estimate) FILTER (WHERE pff_variable IN ('{{ brackets_under_150k | join("', '") }}'))
    + (
        SELECT (median_amount - 150000) / 50000
        FROM {{ ref('stg_tcol__budget_components') }}
        WHERE broad_family_type = 'Adults under 65, with children' AND component = 'Total Cost'
    ) * sum(estimate) FILTER (WHERE pff_variable = 'fi150t199') AS families_income_under_tcol_acs
{%- endmacro %}
