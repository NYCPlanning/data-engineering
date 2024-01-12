-- spatial aggregates
-- this file is templated and called by python/unit_change_summary
-- one output is created with changes in units by year per each relevant geometry

DROP TABLE IF EXISTS aggregate_{{ geom }} CASCADE;
WITH agg AS (
    SELECT
        {{ source_column }} AS {{ output_column }},

        SUM(comp2020ap) AS comp2020ap,
        {%- for year in years %}
            SUM(comp{{ year }}) AS comp{{ year }},
        {% endfor %}

        SUM(filed) AS filed,
        SUM(approved) AS approved,
        SUM(permitted) AS permitted,
        SUM(withdrawn) AS withdrawn,
        SUM(inactive) AS inactive

    FROM yearly_devdb

    GROUP BY 
        {%- for column in group_by %}
            {{ column ~ ", " if not loop.last else column }}
        {% endfor %}
)
SELECT
    j.{{ geom_join_column }}::TEXT AS "{{ output_column }}", -- slightly hacky. Some outputs we want capitalized, but without quotes everythin is lowercase. 
    -- so prior to this point we're only working with lowercase to be consistent with upstream data
    {%- for column_pair in additional_column_mappings %}
        j.{{ column_pair[0] }} AS {{ column_pair[1] }},
    {% endfor %}
    COALESCE(agg.comp2020ap, 0) AS comp2020ap,
    {%- for year in years %}
        COALESCE(agg.comp{{ year }}, 0) AS comp{{ year }},
    {% endfor %}
    c.hunits AS cenunits20,
    COALESCE(agg.filed, 0) AS filed,
    COALESCE(agg.approved, 0) AS approved,
    COALESCE(agg.permitted, 0) AS permitted,
    COALESCE(agg.withdrawn, 0) AS withdrawn,
    COALESCE(agg.inactive, 0) AS inactive,
    j.shape_area AS "Shape_Area",
    j.shape_leng AS "Shape_Leng",
    j.wkb_geometry

INTO aggregate_{{ geom }}
FROM
    {{ join_table }} AS j
LEFT JOIN agg ON agg.{{ output_column }} = j.{{ geom_join_column }}
LEFT JOIN census2020_housing_units_by_geography AS c ON j.{{ geom_join_column }}::TEXT = c.aggregate_join

ORDER BY j.{{ geom_join_column }};
