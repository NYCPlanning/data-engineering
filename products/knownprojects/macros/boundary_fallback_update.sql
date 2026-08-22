{#
    Assign projects that matched no boundary to one they intersect.

    Carried over from the source scripts' closing UPDATE, which is arbitrary
    when a project intersects several. It previously set only the geography,
    the proportion and units_net, leaving the phased-unit columns NULL, so
    rescued projects contributed nothing to the phasing totals. They are whole
    projects in a single geography, so every measure takes the full value.
#}

{% macro boundary_fallback_update(boundary_table, id_column, suffix) %}
    UPDATE {{ this }} AS a
    SET
        {{ suffix }} = b.{{ id_column }},
        proportion_in_{{ suffix }} = 1,
        units_net_in_{{ suffix }} = a.units_net,
        future_phased_units_total_in_{{ suffix }} = a.future_phased_units_total,
        future_units_without_phasing_in_{{ suffix }} = a.future_units_without_phasing,
        completed_units_in_{{ suffix }} = a.completed_units,
        within_5_years_in_{{ suffix }} = a.within_5_years,
        from_5_to_10_years_in_{{ suffix }} = a.from_5_to_10_years,
        after_10_years_in_{{ suffix }} = a.after_10_years
    FROM {{ source('recipe_sources', boundary_table) }} AS b
    WHERE
        a.{{ suffix }} IS NULL
        AND NOT st_isempty(a.geometry)
        AND st_intersects(a.geometry, b.geometry)
{% endmacro %}
