-- -v VERSION=$VERSION
CREATE TABLE IF NOT EXISTS qaqc_outlier(
    v character varying,
    CONDO character varying,
    MAPPED character varying,
    outlier character varying
);

DELETE FROM qaqc_outlier
WHERE v = :'VERSION'
AND CONDO::boolean = :CONDO
AND MAPPED::boolean = :MAPPED;

INSERT INTO qaqc_outlier (
select :'VERSION' as v, 	 
       :CONDO as condo,
       :MAPPED as mapped,
       jsonb_agg(t) as outlier
from (
	select jsonb_agg(json_build_object('bbl', tmp.bbl, 'unitsres',tmp.unitsres, 'resarea', tmp.resarea,
                              'res_unit_ratio', tmp.res_unit_ratio)) as values, 'unitsres_resarea' as field
	from (SELECT bbl, unitsres, resarea, resarea::FLOAT/unitsres::FLOAT as res_unit_ratio
          FROM (SELECT a.* 
                FROM archive_pluto a
                :CONDITION) b
          WHERE unitsres::FLOAT>50 and resarea::FLOAT/unitsres::FLOAT < 300
          AND resarea::FLOAT!= 0)tmp
	union
	select jsonb_agg(json_build_object('bbl', tmp.bbl, 'bldgarea',tmp.bldgarea, 'lotarea', tmp.lotarea,
                              'numfloors', tmp.numfloors, 'blog_lot_ratio', tmp.bldg_lot_ratio)) as values, 'lotarea_numfloor' as field
	from (SELECT bbl, bldgarea, lotarea, numfloors, bldgarea::FLOAT/lotarea::FLOAT as bldg_lot_ratio
          FROM (SELECT a.*
                FROM archive_pluto a
                :CONDITION) b
          WHERE bldgarea::FLOAT/lotarea::FLOAT > numfloors::FLOAT*2
          AND lotarea::FLOAT != 0 AND numfloors !=0)tmp
    union 
    select jsonb_agg(json_build_object('pair',m.pair, 'bbl', m.bbl,
                                       'building_area_current',m.building_area_current,
                                       'building_area_previous',m.building_area_previous,
                                       'percent_change',m.percent_change)) as values, 
                                       'building_area_increase' as field
    from (SELECT :'VERSION'||' - '||:'VERSION_PREV' as pair,
                a.bbl, a.lotarea::FLOAT as building_area_current, b.lotarea::FLOAT as building_area_previous, 
                ((a.lotarea::FLOAT-b.lotarea::FLOAT)/b.lotarea::FLOAT * 100) as percent_change
          FROM (SELECT a.* 
                FROM archive_pluto a
                :CONDITION) a, previous_pluto b
          WHERE a.bbl::FLOAT = b.bbl::FLOAT AND a.lotarea::FLOAT > 2*b.lotarea::FLOAT
          AND b.lotarea::FLOAT != 0) m
      )t);