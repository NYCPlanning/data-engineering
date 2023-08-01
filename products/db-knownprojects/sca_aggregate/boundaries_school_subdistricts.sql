/**********************************************************************************************************************************************************************************
Sources: _kpdb - finalized version of KPDB build 
		 doe_schoolsubdistricts
OUTPUT: longform_subdist_output_cp_assumptions

*******************************************************************************************************************************************/

drop table if exists aggregated_subdist_cp_assumptions;
drop table if exists ungeocoded_PROJEcts_subdist_cp_assumptions;
drop table if exists aggregated_subdist_longform_cp_assumptions;
drop table if exists aggregated_subdist_PROJEct_level_cp_assumptions;
drop table if exists longform_subdist_output_cp_assumptions;

SELECT
	*
into
	aggregated_subdist_cp_assumptions
from (
	with aggregated_boundaries_subdist as (
		SELECT
		--	a.cartodb_id,
			a.geometry,
		--	a.geometry_webmercator,
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
			b.geometry as subdist_geom,
			b.district as distzone,
			b.subdistrict as subdistzone,
			b.name as a_dist_zone_name,
			st_distance(a.geometry::geography,b.geometry::geography) as subdist_Distance
		from
			_kpdb a
		left join
			doe_school_subdistricts b
		on 
		case
			/*Treating large developments as polygons*/
			when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('EDC Projected Projects','DCP Application','DCP Planner-Added Projects')	then
				st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating subdivisions in SI across many lots as polygons*/
			when a.record_id in(SELECT record_id FROM zap_project_many_bbls) and a.record_name like '%SD %'								then
				st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
			when a.record_name like '%Resilient Housing%' and a.source in('DCP Application','DCP Planner-Added Projects')									then
				st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
			when (a.record_name like '%NIHOP%' or a.record_name like '%NCP%' )and a.source in('DCP Application','DCP Planner-Added Projects')	then
				st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

			/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
			when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') 														then
				st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1


			/*Treating other polygons as points, using their centroid*/
			when st_area(a.geometry) > 0 																											then
				st_INTERSECTs(st_centroid(a.geometry),b.geometry) 

			/*Treating points as points*/
			else
				st_INTERSECTs(a.geometry,b.geometry) 																								end
	/*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
	),

	multi_geocoded_PROJECTs as (
		SELECT
			source,
			record_id
		from
			aggregated_boundaries_subdist
		group by
			source,
			record_id
		having
			count(*)>1
	),

	aggregated_boundaries_subdist_2 as (
		SELECT
			a.*,
			case when 	concat(a.source,a.record_id) in(SELECT concat(source,record_id) from multi_geocoded_PROJECTs) and st_area(a.geometry) > 0	then 
						CAST(ST_Area(ST_INTERSECTion(a.geometry,a.subdist_geom))/ST_Area(a.geometry) AS DECIMAL) 										else
						1 end																														as proportion_in_subdist
		from
			aggregated_boundaries_subdist a
	),

	aggregated_boundaries_subdist_3 as (
		SELECT
			source,
			record_id,
			sum(proportion_in_subdist) as total_proportion
		from
			aggregated_boundaries_subdist_2
		group by
			source,
			record_id
	),

	aggregated_boundaries_subdist_4 as (
		SELECT
			a.*,
			case when b.total_proportion is not null then cast(a.proportion_in_subdist/b.total_proportion as decimal)
				else 1 			  end as proportion_in_subdist_1,
			case when b.total_proportion is not null then round(a.units_net * cast(a.proportion_in_subdist/b.total_proportion as decimal)) 
				else a.units_net end as units_net_1
		from
			aggregated_boundaries_subdist_2 a
		left join
			aggregated_boundaries_subdist_3 b
		on
			a.record_id = b.record_id and a.source = b.source
	)

	SELECT * from aggregated_boundaries_subdist_4

) as _1;



/*Identify projects which did not geocode to any Subdistrict*/

SELECT
	*
into
	ungeocoded_PROJECTs_subdist_cp_assumptions
from (
	with ungeocoded_PROJECTs_subdist as (
		SELECT
			a.*,
			coalesce(a.distzone,b.district) as distzone_1,
			coalesce(a.subdistzone, b.subdistrict) as subdistzone_1,
			coalesce(a.a_dist_zone_name,b.name) as a_dist_zone_name_1,
			coalesce(
						a.subdist_distance,
						st_distance(
									b.geometry::geography,
									case
										when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added Projects') 	then a.geometry::geography
										when st_area(a.geometry) > 0 																										then st_centroid(a.geometry)::geography
										else a.geometry::geography 																											end
									)
					) as subdist_distance1
		from
			aggregated_subdist_cp_assumptions a 
		left join
			doe_school_subdistricts b
		on 
			a.subdist_distance is null and
			case
				when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added Projects') 		then
					st_dwithin(a.geometry::geography,b.geometry::geography,500)
				when st_area(a.geometry) > 0 																											then
					st_dwithin(st_centroid(a.geometry)::geography,b.geometry::geography,500)
				else
					st_dwithin(a.geometry::geography,b.geometry::geography,500)																			end
	)
	SELECT * from ungeocoded_PROJECTs_subdist
) as _2;



drop table if exists aggregated_subdist_longform_cp_assumptions;

SELECT
	*
into
	aggregated_subdist_longform_cp_assumptions
from (
	with min_distances as (
		SELECT
			record_id,
			min(subdist_distance1) as min_distance
		from
			ungeocoded_PROJECTs_subdist_cp_assumptions
		group by 
			record_id
	),

	all_PROJECTs_subdist as (
		SELECT
			a.*
		from
			ungeocoded_PROJECTs_subdist_cp_assumptions a 
		inner join
			min_distances b
		on
			a.record_id = b.record_id and
			a.subdist_distance1=b.min_distance
	)

	SELECT 
		a.*, 
		b.distzone_1 as distzone,
		b.subdistzone_1 as subdistzone,
		b.a_dist_zone_name_1 as a_dist_zone_name,
		b.proportion_in_subdist_1 as proportion_in_subdist,
		round(a.units_net * b.proportion_in_subdist_1) as units_net_in_subdist
	from 
		_kpdb a 
	left join 
		all_PROJECTs_subdist b 
	on 
		a.source = b.source and 
		a.record_id = b.record_id 
	order by 
		source asc,
		record_id asc,
		record_name asc,
		status asc,
		b.distzone_1 asc,
		b.subdistzone_1 asc
) as _3
	order by distzone asc;


SELECT
	*
into
	aggregated_subdist_PROJECT_level_cp_assumptions
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
						nullif(distzone,''),
						concat(round(100*proportion_in_subdist,0),'%')
					),
				'')),
		' | ') 	as distzone,
		array_to_string(
			array_agg(
				nullif(
					concat_ws
					(
						': ',
						nullif(a_dist_zone_name,''),
						concat(round(100*proportion_in_subdist,0),'%')
					),
				'')),
		' | ') 	as a_dist_zone_name ,
		geometry
		--geometry_webmercator
	from
		aggregated_subdist_longform_cp_assumptions
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
) x;

/*
	Output final subdistrict KPDB. This is not at the project-level, but rather the project & subdist-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.

  	
  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations

*/

-- this is a bit fragile - if remaining unassigned projects overlapped with multiple, this would have undesired behavior
-- quick fix on 3/29/23 to fix 43 records not matching. Verified via manual querying that this has desired outcome
-- currently not fixing any records, so commenting out
/*
UPDATE aggregated_subdist_longform_cp_assumptions a 
    SET
        distzone = b.district,
		subdistzone = b.subdistrict,
		a_dist_zone_name = b.name,
        proportion_in_csd = 1,
        units_net_in_csd = a.units_net,
FROM dcp_school_districts b 
WHERE a.distzone IS NULL 
    AND a.subdistzone IS NULL
    AND NOT st_isempty(a.geometry)
    AND st_intersects(a.geometry, b.geometry);
*/

SELECT
	*
into
	longform_subdist_output_cp_assumptions
from (
	SELECT * FROM aggregated_subdist_longform_cp_assumptions 
	-- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
) x;