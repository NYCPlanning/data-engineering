-- spatial aggregates
-- this file is templated and called by python/unit_change_summary
-- one output is created with changes in units by year per each relevant geometry

DROP TABLE IF EXISTS aggregate_{{ geom }} CASCADE;
WITH agg AS (
    SELECT
        {{ source_column }} AS {{ output_column }},

        sum(comp2020ap) AS comp2020ap,
        {%- for year in years %}
            sum(comp{{ year }}) AS comp{{ year }},
        {% endfor %}

        sum(filed) AS filed,
        sum(approved) AS approved,
        sum(permitted) AS permitted,
        sum(withdrawn) AS withdrawn,
        sum(inactive) AS inactive

    FROM yearly_devdb

    GROUP BY 
    {%- for column in group_by %}
        {{ column }}{{ ", " if not loop.last else "" }} -- noqa
    {% endfor %}
)
SELECT
    j.{{ geom_join_column }}::text AS "{{ output_column }}", -- slightly hacky. Some outputs we want capitalized, but without quotes everythin is lowercase. 
    -- so prior to this point we're only working with lowercase to be consistent with upstream data
    {%- for column_pair in additional_column_mappings %}
        j.{{ column_pair[0] }} AS {{ column_pair[1] }},
    {% endfor %}
    coalesce(agg.comp2020ap, 0) AS comp2020ap,
    {%- for year in years %}
        coalesce(agg.comp{{ year }}, 0) AS comp{{ year }},
    {% endfor %}
    c.hunits AS cenunits20,
    coalesce(agg.filed, 0) AS filed,
    coalesce(agg.approved, 0) AS approved,
    coalesce(agg.permitted, 0) AS permitted,
    coalesce(agg.withdrawn, 0) AS withdrawn,
    coalesce(agg.inactive, 0) AS inactive,
    j.shape_area AS "Shape_Area",
    j.shape_leng AS "Shape_Leng",
    j.wkb_geometry

INTO aggregate_{{ geom }}
FROM {{ join_table }} AS j
LEFT JOIN agg ON agg.{{ output_column }} = j.{{ geom_join_column }}
LEFT JOIN census2020_housing_units_by_geography AS c ON j.{{ geom_join_column }}::text = c.aggregate_join

ORDER BY j.{{ geom_join_column }};
