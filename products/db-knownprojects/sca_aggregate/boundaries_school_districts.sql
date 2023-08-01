/**********************************************************************************************************************************************************************************
Sources: _kpdb - finalized version of KPDB build 
         dcp_school_districts
OUTPUT: longform_csd_output
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_csd;
drop table if exists ungeocoded_PROJECTs_CSD;
drop table if exists aggregated_CSD_longform;
drop table if exists aggregated_CSD_PROJECT_level;
drop table if exists longform_csd_output; 


SELECT
	*
into
	aggregated_CSD
from (
	WITH aggregated_boundaries_CSD AS (
		SELECT
			--a.cartodb_id,
			a.geometry,
			--a.geometry_webmercator,
			a.project_id,
			a.source,
			a.record_id,
			a.record_name,
			a.borough,
			a.status,
			a.type,
			a.date,
			a.date_type,
			a.units_gross,
			a.units_net,
			a.prop_within_5_years,
			a.prop_5_to_10_years,
			a.prop_after_10_years,
			a.within_5_years,
			a.from_5_to_10_years,
			a.after_10_years,
			a.phasing_rationale,
			a.phasing_known,
			a.nycha,
			a.classb, 
			a.senior_housing,
			a.inactive,
			b.geometry as CSD_geom,
			b.SCHOOLDIST AS CSD,
			st_distance(a.geometry::geography,b.geometry::geography) as CSD_Distance
		from
			-- capitalplanning.kpdb_2021_09_10_nonull a
			_kpdb a
		left join
			dcp_school_districts b
		on 
		case
			/*Treating large developments as polygons*/
			when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('EDC Projected Projects','DCP Application','DCP Planner-Added Projects')	then
			/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
				st_INTERSECTs(a.geometry,b.geometry) AND CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating subdivisions in SI across many lots as polygons*/
			when a.record_id in(SELECT record_id from zap_project_many_bbls) and a.record_name like '%SD %' then
			/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
				st_INTERSECTs(a.geometry,b.geometry) AND CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
			when a.record_name like '%Resilient Housing%' and a.source in('DCP Application','DCP Planner-Added PROJECTs') then
			/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
				st_INTERSECTs(a.geometry,b.geometry) AND CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
			when (a.record_name like '%NIHOP%' or a.record_name like '%NCP%' )and a.source in('DCP Application','DCP Planner-Added PROJECTs') then
			/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
				st_INTERSECTs(a.geometry,b.geometry) AND CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
			when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') then
			/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
				st_INTERSECTs(a.geometry,b.geometry) AND CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1
			/*Treating other polygons as points, using their centroid*/
			
			/*Treating other polygons as points, using their centroid*/
			when st_area(a.geometry) > 0 then
				st_INTERSECTs(st_centroid(a.geometry),b.geometry) 

			/*Treating points as points*/
			else
				st_INTERSECTs(a.geometry,b.geometry) end
			/*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
	),

	/*Identify projects geocoded to multiple CSDs*/
	multi_geocoded_PROJECTs as (
		SELECT
			source,
			record_id
		from
			aggregated_boundaries_CSD
		group by
			source,
			record_id
		having
			count(*)>1
	),

	/*Calculate the proportion of each project in each CSD that it overlaps with*/
	aggregated_boundaries_CSD_2 as (
		SELECT
			a.*,
			case when 	concat(a.source,a.record_id) in(SELECT concat(source,record_id) from multi_geocoded_PROJECTs) and st_area(a.geometry) > 0	then 
						CAST(ST_Area(ST_INTERSECTion(a.geometry,a.CSD_geom))/ST_Area(a.geometry) AS DECIMAL) else
						1 end	as proportion_in_CSD
		from
			aggregated_boundaries_CSD a
	),

	/*
	  If <10% of a project falls into a particular CSD, then the sum of all proportions of a project in each CSD would be <100%, because
	  projects with less than 10% in a CSD are not assigned to that CSD. The next two steps ensure that 100% of each project's units are
	  allocated to a CSD.
	*/
	aggregated_boundaries_CSD_3 as (
		SELECT
			source,
			record_id,
			sum(proportion_in_CSD) as total_proportion
		from
			aggregated_boundaries_CSD_2
		group by
			source,
			record_id
	),

	aggregated_boundaries_CSD_4 as (
		SELECT
			a.*,
			case when b.total_proportion is not null then cast(a.proportion_in_CSD/b.total_proportion as decimal)
				else 1 			  end as proportion_in_CSD_1,
			case when b.total_proportion is not null then round(a.units_net * cast(a.proportion_in_CSD/b.total_proportion as decimal)) 
				else a.units_net end as counted_units_1
		from
			aggregated_boundaries_CSD_2 a
		left join
			aggregated_boundaries_CSD_3 b
		on
			a.record_id = b.record_id and a.source = b.source
	)

	SELECT * from aggregated_boundaries_CSD_4
) as _1;


/*Identify projects which did not geocode to any CSD*/
SELECT
	*
into
	ungeocoded_PROJECTs_CSD
from (
	with ungeocoded_PROJECTs_CSD as (
		SELECT
			a.*,
			coalesce(a.CSD,b.schooldist) as CSD_1,
			coalesce(
						a.CSD_distance,
						st_distance(
									CSD_geom::geography,
									case
										when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added PROJECTs') 	then a.geometry::geography
										when st_area(a.geometry) > 0 																										then st_centroid(a.geometry)::geography
										else a.geometry::geography 																											end
									)
					) as CSD_distance1
		from
			aggregated_CSD a 
		left join
			dcp_school_districts b
		on 
			a.CSD_distance is null and
			case
				when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added PROJECTs') 		then
					st_dwithin(a.geometry::geography,CSD_geom::geography,500)
				when st_area(a.geometry) > 0 																											then
					st_dwithin(st_centroid(a.geometry)::geography,CSD_geom::geography,500)
				else
					st_dwithin(a.geometry::geography,CSD_geom::geography,500)																			end
	)
	SELECT * from ungeocoded_PROJECTs_CSD
) as _2;

/*Assign ungeocoded projects to their closest CSD*/

SELECT
	*
into
	aggregated_CSD_longform
from (
	with min_distances as (
		SELECT
			record_id,
			min(CSD_distance1) as min_distance
		from
			ungeocoded_PROJECTs_CSD
		group by 
			record_id
	),

	all_PROJECTs_CSD as (
		SELECT
			a.*
		from
			ungeocoded_PROJECTs_CSD a 
		inner join
			min_distances b
		on
			a.record_id = b.record_id and
			a.CSD_distance1=b.min_distance
	)

	SELECT 
		a.*, 
		b.CSD_1 as CSD, 
		b.proportion_in_CSD_1 as proportion_in_CSD,
		round(a.units_net * b.proportion_in_CSD_1) as units_net_in_CSD 
	from 
		-- capitalplanning.kpdb_2021_09_10_nonull a 
		_kpdb a
	left join 
		all_PROJECTs_CSD b 
	on 
		a.source = b.source and 
		a.record_id = b.record_id 
	order by 
		source asc,
		record_id asc,
		record_name asc,
		status asc,
		b.CSD_1 asc
) as _3
order by csd asc;

/*Aggregate all results to the project-level, because if a project matches with multiple CSDs, it'll appear in multiple rows*/

SELECT
	*
into
	aggregated_CSD_PROJECT_level
from (
	SELECT
		source,
		record_id,
		record_name,
		type,
		inactive,
		status,
		borough,
		units_gross,
		units_net,
		prop_within_5_years,
		prop_5_to_10_years,
		prop_after_10_years,
		within_5_years,
		from_5_to_10_years,
		after_10_years,
		phasing_rationale,
		phasing_known,
		date,
		date_type,
		nycha,
		classb,
		senior_housing,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(concat(CSD),''),
						concat(round(100*proportion_in_csd,0),'%')
					),
				'')),
		' | ') 	as CSD,
		geometry
		--geometry_webmercator 
	from
		(select * from aggregated_CSD_longform order by csd asc) a
	group by
		geometry,
		--geometry_webmercator,
		source,
		record_id,
		record_name,
		type,
		inactive,
		status,
		borough,
		units_gross,
		units_net,
		prop_within_5_years,
		prop_5_to_10_years,
		prop_after_10_years,
		within_5_years,
		from_5_to_10_years,
		after_10_years,
		phasing_rationale,
		phasing_known,
		date,
		date_type,
		nycha,
		classb,
		senior_housing
) x
;

-- this is a bit fragile - if remaining unassigned projects overlapped with multiple, this would have undesired behavior
-- quick fix on 3/29/23 to fix 43 records not matching. Verified via manual querying that this has desired outcome
UPDATE aggregated_CSD_longform a 
    SET
        CSD = b.schooldist,
        proportion_in_csd = 1,
        units_net_in_csd = a.units_net
FROM dcp_school_districts b 
WHERE a.CSD IS NULL
    AND NOT st_isempty(a.geometry)
    AND st_intersects(a.geometry, b.geometry);

/*
	Output final CSD-based KPDB. This is not at the project-level, but rather the project & CSD-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.


  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations

*/

SELECT
	*
into
	longform_csd_output
from (
	SELECT *  FROM aggregated_csd_longform 
	-- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
		order by 
			source asc,
			record_id asc,
			record_name asc,
			status asc
) x;


-- select cdb_cartodbfytable('capitalplanning','longform_csd_output'); -- not necessary to run next script