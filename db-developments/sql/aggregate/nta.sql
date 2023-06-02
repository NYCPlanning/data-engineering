DROP TABLE IF EXISTS AGGREGATE_nta_{{ decade }};
SELECT 
    nta{{ decade }},
    SUM(comp2010ap) as comp2010ap,
    {%- for year in years %}
    SUM(comp{{year}}) as comp{{year}},
    {% endfor %}
    SUM(filed) as filed,
    SUM(approved) as approved,
    SUM(permitted) as permitted,
    SUM(withdrawn) as withdrawn,
    SUM(inactive) as inactive

    -- {% if decade == '2010' %}

    --     ,SUM(CENSUS_by_tract.cenunits10) as cenunits10
    --     ,SUM(COALESCE(YEARLY_devdb_{{ decade }}.since_cen10, 0) + COALESCE(CENSUS_by_tract.cenunits10, 0)) as total
    --     ,SUM(census_units10adj.adjunits10) as adjunits10
    --     ,SUM(COALESCE(YEARLY_devdb_{{ decade }}.since_cen10, 0) + COALESCE(census_units10adj.adjunits10, 0)) as totaladj
    
    -- {% endif %}

INTO AGGREGATE_nta_{{ decade }}
FROM YEARLY_devdb_{{ decade }}

-- {% if decade == '2010' %}

--     LEFT JOIN (
--         SELECT centract10, SUM(cenunits10) as cenunits10
--         FROM census_units10
--         GROUP BY centract10
--     ) CENSUS_by_tract 
--         ON YEARLY_devdb_{{ decade }}.centract2010 = CENSUS_by_tract.centract10
--     LEFT JOIN census_units10adj 
--         ON YEARLY_devdb_{{ decade }}.centract2010 = census_units10adj.centract10

-- {% endif %}

GROUP BY 
    nta{{ decade }}
ORDER BY 
    nta{{ decade }};
