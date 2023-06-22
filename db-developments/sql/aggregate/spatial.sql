DROP TABLE IF EXISTS AGGREGATE_{{ geom }}_{{ decade }};
WITH agg as (
    SELECT 
        {{ source_column }}::TEXT as {{ output_column }},

        SUM(comp2010ap) as comp2010ap,
        {%- for year in years %}
            SUM(comp{{year}}) as comp{{year}},
        {% endfor %}

        SUM(filed) as filed,
        SUM(approved) as approved,
        SUM(permitted) as permitted,
        SUM(withdrawn) as withdrawn,
        SUM(inactive) as inactive

    FROM YEARLY_devdb_{{ decade }}

    GROUP BY 
        {%- for column in group_by %}
            {{column}}{{ ", " if not loop.last else "" }}
        {% endfor %}
)
SELECT 
    j.{{ right_join_column }} as {{ output_column }},
    {%- for column in additional_columns %} -- These are grabbed from joined table in case of no rows present in prior table
        j.{{ column[0] }} as {{ column[1] }},
    {% endfor %}
    agg.comp2010ap,
    {%- for year in years %}
        agg.comp{{year}},
    {% endfor %}
    agg.filed,
    agg.approved,
    agg.permitted,
    agg.withdrawn,
    agg.inactive,
    j.shape_area as Shape_Area,
    j.shape_leng as Shape_Leng,
    j.wkb_geometry

INTO AGGREGATE_{{ geom }}_{{ decade }}
FROM
    agg
    RIGHT JOIN {{ join_table }} j ON agg.{{ output_column }} = j.{{ right_join_column }}
