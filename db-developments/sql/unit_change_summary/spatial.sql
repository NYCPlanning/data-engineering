DROP TABLE IF EXISTS aggregate_{{ geom }} CASCADE;
WITH agg AS (
    SELECT 
        {{ source_column }} AS {{ output_column }},

        SUM(comp2020ap) AS comp2020ap,
        {%- for year in years %}
            SUM(comp{{year}}) AS comp{{year}},
        {% endfor %}

        SUM(filed) AS filed,
        SUM(approved) AS approved,
        SUM(permitted) AS permitted,
        SUM(withdrawn) AS withdrawn,
        SUM(inactive) AS inactive

    FROM YEARLY_devdb

    GROUP BY 
        {%- for column in group_by %}
            {{column}}{{ ", " if not loop.last else "" }}
        {% endfor %}
)
SELECT 
    j.{{ geom_join_column }}::TEXT AS {{ output_column }},
    {%- for column_pair in additional_column_mappings %} -- These are grabbed from joined table in case of no rows present in prior table
        j.{{ column_pair[0] }} AS {{ column_pair[1] }},
    {% endfor %}
    coalesce(agg.comp2020ap, 0) AS comp2020ap,
    {%- for year in years %}
        coalesce(agg.comp{{year}}, 0) AS comp{{year}},
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
FROM
    agg
    RIGHT JOIN {{ join_table }} j ON agg.{{ output_column }} = j.{{ geom_join_column }}
    LEFT JOIN census2020_housing_units_by_geography c ON j.{{ geom_join_column }}::TEXT = c.aggregate_join

ORDER BY j.{{ geom_join_column }};


-- external export - not include current year if building 22Q4 in 2023
CREATE VIEW aggregate_{{ geom }}_external AS SELECT
    {{ output_column }},
    {%- for column_pair in additional_column_mappings %} 
        {{ column_pair[1] }},
    {% endfor %}
    comp2020ap,
    {%- for year in years %}
        {% if (not loop.last) or include_current_year %}
            comp{{year}},
        {% endif %}
    {% endfor %}
    cenunits20,
    filed,
    approved,
    permitted,
    withdrawn,
    inactive,
    "Shape_Area",
    "Shape_Leng",
    wkb_geometry
    FROM aggregate_{{ geom }};
