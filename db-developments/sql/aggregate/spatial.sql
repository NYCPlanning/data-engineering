DROP TABLE IF EXISTS aggregate_{{ geom }} CASCADE;
WITH agg as (
    SELECT 
        {{ source_column }}::TEXT as {{ output_column }},

        SUM(comp2020ap) as comp2020ap,
        {%- for year in years %}
            SUM(comp{{year}}) as comp{{year}},
        {% endfor %}

        SUM(filed) as filed,
        SUM(approved) as approved,
        SUM(permitted) as permitted,
        SUM(withdrawn) as withdrawn,
        SUM(inactive) as inactive

    FROM YEARLY_devdb

    GROUP BY 
        {%- for column in group_by %}
            {{column}}{{ ", " if not loop.last else "" }}
        {% endfor %}
)
SELECT 
    j.{{ geom_join_column }} as {{ output_column }},
    {%- for column in additional_columns %} -- These are grabbed from joined table in case of no rows present in prior table
        j.{{ column[0] }} as {{ column[1] }},
    {% endfor %}
    coalesce(agg.comp2020ap, 0) AS comp2020ap,
    {%- for year in years %}
        coalesce(agg.comp{{year}}, 0) AS comp{{year}},
    {% endfor %}
    coalesce(census.{{ "\"hunits.1\"" if geom == "block" else "HUnits_20" }}::int, 0) AS cenunits20,
    coalesce(agg.filed, 0) AS filed,
    coalesce(agg.approved, 0) AS approved,
    coalesce(agg.permitted, 0) AS permitted,
    coalesce(agg.withdrawn, 0) AS withdrawn,
    coalesce(agg.inactive, 0) AS inactive,
    j.shape_area as Shape_Area,
    j.shape_leng as Shape_Leng,
    j.wkb_geometry

INTO aggregate_{{ geom }}
FROM
    agg
    RIGHT JOIN {{ join_table }} j ON agg.{{ output_column }} = j.{{ geom_join_column }}
    LEFT JOIN {{ "dcp_censusdata_blocks" if geom == "block" else "dcp_censusdata" }} census
        ON j.{{ geom_join_column }} = census.{{ census_join_column }}

ORDER BY j.{{ geom_join_column }}::text;

-- Views to simplify export

-- internal export - include current year even if export is Q4 of prior year, exclude geom
CREATE VIEW aggregate_{{ geom }}_internal AS SELECT
    {{ output_column }} AS {{ output_column_internal }},
    {%- for column in additional_columns %} 
        {{ column[1] }}, -- TODO add alias here if needed
    {% endfor %}
    comp2020ap,
    {%- for year in years %}
        {% if not loop.last %}
            comp{{year}},
        {% endif %}
    {% endfor %}
    cenunits20,
    filed,
    approved,
    permitted,
    withdrawn,
    inactive
    FROM aggregate_{{ geom }};

-- external export for shapefile
CREATE VIEW aggregate_{{ geom }}_external AS SELECT
    {{ output_column }}::integer,
    {%- for column in additional_columns %} 
        {{ column[1] }},
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
    Shape_Area,
    Shape_Leng,
    wkb_geometry
    FROM aggregate_{{ geom }};