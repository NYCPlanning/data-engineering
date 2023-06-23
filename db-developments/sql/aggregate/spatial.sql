DROP TABLE IF EXISTS aggregate_{{ geom }}_{{ decade }};
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
    coalesce(agg.comp2010ap, 0) AS comp2010ap,
    {%- for year in years %}
        coalesce(agg.comp{{year}}, 0) AS comp{{year}},
    {% endfor %}
    coalesce(agg.filed, 0) AS filed,
    coalesce(agg.approved, 0) AS approved,
    coalesce(agg.permitted, 0) AS permitted,
    coalesce(agg.withdrawn, 0) AS withdrawn,
    coalesce(agg.inactive, 0) AS inactive,
    j.shape_area as Shape_Area,
    j.shape_leng as Shape_Leng,
    j.wkb_geometry

INTO aggregate_{{ geom }}_{{ decade }}
FROM
    agg
    RIGHT JOIN {{ join_table }} j ON agg.{{ output_column }} = j.{{ right_join_column }}

ORDER BY j.{{ right_join_column }};

-- Views to simplify export
DROP VIEW IF EXISTS aggregate_{{ geom }}_{{ decade }}_shp;

-- internal export - include current year even if export is Q4 of prior year, exclude geom
DROP VIEW IF EXISTS aggregate_{{ geom }}_{{ decade }}_internal;
CREATE VIEW aggregate_{{ geom }}_{{ decade }}_internal AS SELECT
    {{ output_column }} AS {{ output_column_internal }},
    {%- for column in additional_columns %} 
        {{ column[1] }}, -- TODO add alias here if needed
    {% endfor %}
    comp2010ap,
    {%- for year in years %}
        {% if (not loop.last) or include_current_year %}
            comp{{year}},
        {% endif %}
    {% endfor %}
    filed,
    approved,
    permitted,
    withdrawn,
    inactive
    FROM aggregate_{{ geom }}_{{ decade }};

-- external export csv - drop current year if export is for last year, drop geom
DROP VIEW IF EXISTS aggregate_{{ geom }}_{{ decade }}_external;
CREATE VIEW aggregate_{{ geom }}_{{ decade }}_external AS SELECT
    {{ output_column }},
    {%- for column in additional_columns %} 
        {{ column[1] }},
    {% endfor %}
    comp2010ap,
    {%- for year in years %}
        {% if (not loop.last) or include_current_year %}
            comp{{year}},
        {% endif %}
    {% endfor %}
    filed,
    approved,
    permitted,
    withdrawn,
    inactive
    FROM aggregate_{{ geom }}_{{ decade }};

-- external export for shapefile
CREATE VIEW aggregate_{{ geom }}_{{ decade }}_shp AS SELECT
    {{ output_column }},
    {%- for column in additional_columns %} 
        {{ column[1] }},
    {% endfor %}
    comp2010ap,
    {%- for year in years %}
        {% if (not loop.last) or include_current_year %}
            comp{{year}},
        {% endif %}
    {% endfor %}
    filed,
    approved,
    permitted,
    withdrawn,
    inactive,
    Shape_Area,
    Shape_Leng,
    wkb_geometry
    FROM aggregate_{{ geom }}_{{ decade }};