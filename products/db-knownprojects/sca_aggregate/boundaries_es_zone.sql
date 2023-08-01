/*******************************************************************************************************************************************
Sources: _kpdb - Finalized version of KPDB build 
		 doe_eszones

OUTPUT: longform_es_zone_output
*******************************************************************************************************************************************/

drop table if exists aggregated_es_zone;
drop table if exists ungeocoded_projects_es_zone;
drop table if exists aggregated_es_zone_longform;
drop table if exists aggregated_es_zone_project_level;
drop table if exists longform_es_zone_output;


SELECT
	*
into
	aggregated_es_zone
from
(
	with aggregated_boundaries_es_zone as
(
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
		b.geometry as es_zone_geom,
		b.dbn AS es_zone,
		b.remarks as es_remarks,
		st_distance(a.geometry::geography,b.geometry::geography) as es_zone_Distance
	from
		_kpdb a
	left join
		doe_eszones b
	on 
	case
		/*Treating large developments as polygons*/
		when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('EDC Projected Projects','DCP Application','DCP Planner-Added Projects')	then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

		/*Treating subdivisions in SI across many lots as polygons*/
		when a.record_id in(SELECT record_id FROM zap_project_many_bbls) 
		    and a.record_name like '%SD %' then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

		/*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/ 
		when a.record_name like '%Resilient Housing%' and a.source in('DCP Application','DCP Planner-Added PROJECTs') then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

		/*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/ 
		when (a.record_name like '%NIHOP%' or a.record_name like '%NCP%' )and a.source in('DCP Application','DCP Planner-Added PROJECTs') then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

		/*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
		when a.source in('Future Neighborhood Studies','Neighborhood Study Projected Development Sites') then
		/*Only distribute units to a geography if at least 10% of the project is within that boundary*/
			st_INTERSECTs(a.geometry,b.geometry) and CAST(ST_Area(ST_INTERSECTion(a.geometry,b.geometry))/ST_Area(a.geometry) AS DECIMAL) >= .1

		/*Treating other polygons as points, using their centroid*/
		when st_area(a.geometry) > 0 	then
			st_INTERSECTs(st_centroid(a.geometry),b.geometry) 

		/*Treating points as points*/
		else
			st_INTERSECTs(a.geometry,b.geometry) end
		/*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
),

	/*Identify projects geocoded to multiple ESZs*/
	multi_geocoded_PROJECTs as
(
	SELECT
		source,
		record_id
	from
		aggregated_boundaries_es_zone
	group by
		source,
		record_id
	having
		count(*)>1
),




	/*Calculate the proportion of each project in each ES Zone that it overlaps with*/
	aggregated_boundaries_es_zone_2 as
(
	SELECT
		a.*,
		case when 	concat(a.source,a.record_id) in(SELECT concat(source,record_id) from multi_geocoded_PROJECTs) and st_area(a.geometry) > 0	then 
					CAST(ST_Area(ST_INTERSECTion(a.geometry,a.es_zone_geom))/ST_Area(a.geometry) AS DECIMAL) 										else
					1 end	as proportion_in_es_zone
	from
		aggregated_boundaries_es_zone a
),

	/*
	  If <10% of a project falls into a particular ES Zone, then the sum of all proportions of a project in each zone would be <100%, because
	  projects with less than 10% in a zone are not assigned to that zone. The next two steps ensure that 100% of each project's units are
	  allocated to a zone.
	*/

	aggregated_boundaries_es_zone_3 as
(
	SELECT
		source,
		record_id,
		sum(proportion_in_es_zone) as total_proportion
	from
		aggregated_boundaries_es_zone_2
	group by
		source,
		record_id
),

	aggregated_boundaries_es_zone_4 as
(
	SELECT
		a.*,
		case when b.total_proportion is not null then cast(a.proportion_in_es_zone/b.total_proportion as decimal)
			 else 1 			  end as proportion_in_es_zone_1,
		case when b.total_proportion is not null then round(a.units_net * cast(a.proportion_in_es_zone/b.total_proportion as decimal)) 
			 else a.units_net end as units_net_1
	from
		aggregated_boundaries_es_zone_2 a
	left join
		aggregated_boundaries_es_zone_3 b
	on
		a.record_id = b.record_id and a.source = b.source
)

	SELECT * from aggregated_boundaries_es_zone_4

) as _1;


/*Identify projects which did not geocode to any ES Zone*/
SELECT
	*
into
	ungeocoded_PROJECTs_es_zone
from
(
	with ungeocoded_PROJECTs_es_zone as
(
	SELECT
		a.*,
		coalesce(a.es_zone,b.dbn) 			as es_zone_1,
		coalesce(a.es_remarks,b.remarks)	as es_remarks_1,
		coalesce(
					a.es_zone_distance,
					st_distance(
								b.geometry::geography,
								case
									when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added PROJECTs') 	then a.geometry::geography
									when st_area(a.geometry) > 0 																							then st_centroid(a.geometry)::geography
									else a.geometry::geography 																							end
								)
				) as es_zone_distance1
	from
		aggregated_es_zone a 
	left join
		doe_eszones b
	on 
		a.es_zone_distance is null and
		case
			when (st_area(a.geometry::geography)>10000 or units_gross > 500) and a.source in('DCP Application','DCP Planner-Added PROJECTs') 	then
				st_dwithin(a.geometry::geography,b.geometry::geography,500)
			when st_area(a.geometry) > 0 																										then
				st_dwithin(st_centroid(a.geometry)::geography,b.geometry::geography,500)
			else
				st_dwithin(a.geometry::geography,b.geometry::geography,500)														end
)
	SELECT * from ungeocoded_PROJECTs_es_zone
) as _2;


SELECT
	*
into
	aggregated_es_zone_longform
from
(
	with	min_distances as
(
	SELECT
		record_id,
		min(es_zone_distance1) as min_distance
	from
		ungeocoded_PROJECTs_es_zone
	group by 
		record_id
),

	all_PROJECTs_es_zone as
(
	SELECT
		a.*
	from
		ungeocoded_PROJECTs_es_zone a 
	inner join
		min_distances b
	on
		a.record_id = b.record_id and
		a.es_zone_distance1=b.min_distance
)

	SELECT 
		a.*, 
		b.es_zone_1 as es_zone, 
		b.es_remarks_1 as es_remarks,
		coalesce(
				b.es_zone_1,
				case 
					when b.es_remarks_1 like '%Contact %' then substring(b.es_remarks_1,1,position('Contact' in b.es_remarks_1) - 1)
					else b.es_remarks_1 end
				)											as es_zone_remarks,
		b.proportion_in_es_zone_1 							as proportion_in_es_zone,
		round(a.units_net * b.proportion_in_es_zone_1) 	as units_net_in_es_zone
	from 
		_kpdb a 
	left join 
		all_PROJECTs_es_zone b 
	on 
		a.source = b.source and 
		a.record_id = b.record_id 
	order by 
		source asc,
		record_id asc,
		record_name asc,
		status asc,
		b.es_zone_1 asc
) as _3
	order by
		es_zone asc;


SELECT
	*
into
	aggregated_es_zone_project_level
from
(
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
						nullif
							(
								es_zone_remarks,
								''
							),
						concat(round(100*proportion_in_es_zone,0),'%')
					),
				'')),
		' | ') 	as es_zone,
		geometry
		--geometry_webmercator
	from
		aggregated_es_zone_longform
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
	Output final ES-zone-based KPDB. This is not at the project-level, but rather the project & ES-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.

  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations
*/

SELECT
	*
into
	longform_es_zone_output
	from
(
SELECT *  FROM aggregated_es_zone_longform 
--where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
	order by 
		source asc,
		record_id asc,
		record_name asc,
		status asc
) x;