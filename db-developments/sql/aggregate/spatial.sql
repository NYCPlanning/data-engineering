DROP TABLE IF EXISTS AGGREGATE_{{ geom }}_{{ decade }};
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

INTO AGGREGATE_{{ geom }}_{{ decade }}
FROM YEARLY_devdb_{{ decade }}

GROUP BY 
    {%- for column in group_by %}

        {{column}}{{ ", " if not loop.last else "" }}
        
    {% endfor %}