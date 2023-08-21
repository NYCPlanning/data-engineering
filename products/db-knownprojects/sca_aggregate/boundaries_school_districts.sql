/**********************************************************************************************************************************************************************************
Sources: _kpdb - finalized version of KPDB build
         dcp_school_districts
OUTPUT: longform_csd_output
*************************************************************************************************************************************************************************************/

drop table if exists aggregated_csd;
drop table if exists ungeocoded_projects_csd;
drop table if exists aggregated_csd_longform;
drop table if exists aggregated_csd_project_level;
drop table if exists longform_csd_output;


select *
into
aggregated_csd
from (
    with aggregated_boundaries_csd as (
        select
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
            b.geometry as csd_geom,
            b.schooldist as csd,
            st_distance(a.geometry::geography, b.geometry::geography) as csd_distance
        from
            -- capitalplanning.kpdb_2021_09_10_nonull a
            _kpdb as a
        left join
            dcp_school_districts as b
            on
                case
                    /*Treating large developments as polygons*/
                    when (st_area(a.geometry::geography) > 10000 or units_gross > 500) and a.source in ('EDC Projected Projects', 'DCP Application', 'DCP Planner-Added Projects')
                        then
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            st_intersects(a.geometry, b.geometry) and (st_area(st_intersection(a.geometry, b.geometry)) / st_area(a.geometry))::decimal >= .1

                    /*Treating subdivisions in SI across many lots as polygons*/
                    when a.record_id in (select record_id from zap_project_many_bbls) and a.record_name like '%SD %'
                        then
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            st_intersects(a.geometry, b.geometry) and (st_area(st_intersection(a.geometry, b.geometry)) / st_area(a.geometry))::decimal >= .1

                    /*Treating Resilient Housing Sandy Recovery PROJECTs, across many DISTINCT lots as polygons. These are three PROJECTs*/
                    when a.record_name like '%Resilient Housing%' and a.source in ('DCP Application', 'DCP Planner-Added PROJECTs')
                        then
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            st_intersects(a.geometry, b.geometry) and (st_area(st_intersection(a.geometry, b.geometry)) / st_area(a.geometry))::decimal >= .1

                    /*Treating NCP and NIHOP projects, which are usually noncontiguous clusters, as polygons*/
                    when (a.record_name like '%NIHOP%' or a.record_name like '%NCP%') and a.source in ('DCP Application', 'DCP Planner-Added PROJECTs')
                        then
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            st_intersects(a.geometry, b.geometry) and (st_area(st_intersection(a.geometry, b.geometry)) / st_area(a.geometry))::decimal >= .1

                    /*Treating neighborhood study projected sites, and future neighborhood studies as polygons*/
                    when a.source in ('Future Neighborhood Studies', 'Neighborhood Study Projected Development Sites')
                        then
                            /*Only distribute units to a geography if at least 10% of the project is within that boundary*/
                            st_intersects(a.geometry, b.geometry) and (st_area(st_intersection(a.geometry, b.geometry)) / st_area(a.geometry))::decimal >= .1
                    /*Treating other polygons as points, using their centroid*/

                    /*Treating other polygons as points, using their centroid*/
                    when st_area(a.geometry) > 0
                        then
                            st_intersects(st_centroid(a.geometry), b.geometry)

                    /*Treating points as points*/
                    else
                        st_intersects(a.geometry, b.geometry)
                end
    /*Only matching if at least 10% of the polygon is in the boundary. Otherwise, the polygon will be apportioned to its other boundaries only*/
    ),

    /*Identify projects geocoded to multiple CSDs*/
    multi_geocoded_projects as (
        select
            source,
            record_id
        from
            aggregated_boundaries_csd
        group by
            source,
            record_id
        having
            count(*) > 1
    ),

    /*Calculate the proportion of each project in each CSD that it overlaps with*/
    aggregated_boundaries_csd_2 as (
        select
            a.*,
            case
                when concat(a.source, a.record_id) in (select concat(source, record_id) from multi_geocoded_projects) and st_area(a.geometry) > 0
                    then
                        (st_area(st_intersection(a.geometry, a.csd_geom)) / st_area(a.geometry))::decimal
                else
                    1
            end as proportion_in_csd
        from
            aggregated_boundaries_csd as a
    ),

    /*
    If <10% of a project falls into a particular CSD, then the sum of all proportions of a project in each CSD would be <100%, because
    projects with less than 10% in a CSD are not assigned to that CSD. The next two steps ensure that 100% of each project's units are
    allocated to a CSD.
    */
    aggregated_boundaries_csd_3 as (
        select
            source,
            record_id,
            sum(proportion_in_csd) as total_proportion
        from
            aggregated_boundaries_csd_2
        group by
            source,
            record_id
    ),

    aggregated_boundaries_csd_4 as (
        select
            a.*,
            case
                when b.total_proportion is not null then (a.proportion_in_csd / b.total_proportion)::decimal
                else 1
            end as proportion_in_csd_1,
            case
                when b.total_proportion is not null then round(a.units_net * (a.proportion_in_csd / b.total_proportion)::decimal)
                else a.units_net
            end as counted_units_1
        from
            aggregated_boundaries_csd_2 as a
        left join
            aggregated_boundaries_csd_3 as b
            on
                a.record_id = b.record_id and a.source = b.source
    )

    select * from aggregated_boundaries_csd_4
) as _1;


/*Identify projects which did not geocode to any CSD*/
select *
into
ungeocoded_projects_csd
from (
    with ungeocoded_projects_csd as (
        select
            a.*,
            coalesce(a.csd, b.schooldist) as csd_1,
            coalesce(
                a.csd_distance,
                st_distance(
                    csd_geom::geography,
                    case
                        when (st_area(a.geometry::geography) > 10000 or units_gross > 500) and a.source in ('DCP Application', 'DCP Planner-Added PROJECTs') then a.geometry::geography
                        when st_area(a.geometry) > 0 then st_centroid(a.geometry)::geography
                        else a.geometry::geography
                    end
                )
            ) as csd_distance1
        from
            aggregated_csd as a
        left join
            dcp_school_districts as b
            on
                a.csd_distance is null
                and case
                    when (st_area(a.geometry::geography) > 10000 or units_gross > 500) and a.source in ('DCP Application', 'DCP Planner-Added PROJECTs')
                        then
                            st_dwithin(a.geometry::geography, csd_geom::geography, 500)
                    when st_area(a.geometry) > 0
                        then
                            st_dwithin(st_centroid(a.geometry)::geography, csd_geom::geography, 500)
                    else
                        st_dwithin(a.geometry::geography, csd_geom::geography, 500)
                end
    )

    select * from ungeocoded_projects_csd
) as _2;

/*Assign ungeocoded projects to their closest CSD*/

select *
into
aggregated_csd_longform
from (
    with min_distances as (
        select
            record_id,
            min(csd_distance1) as min_distance
        from
            ungeocoded_projects_csd
        group by
            record_id
    ),

    all_projects_csd as (
        select a.*
        from
            ungeocoded_projects_csd as a
        inner join
            min_distances as b
            on
                a.record_id = b.record_id
                and a.csd_distance1 = b.min_distance
    )

    select
        a.*,
        b.csd_1 as csd,
        b.proportion_in_csd_1 as proportion_in_csd,
        round(a.units_net * b.proportion_in_csd_1) as units_net_in_csd
    from
        -- capitalplanning.kpdb_2021_09_10_nonull a 
        _kpdb as a
    left join
        all_projects_csd as b
        on
            a.source = b.source
            and a.record_id = b.record_id
    order by
        source asc,
        record_id asc,
        record_name asc,
        status asc,
        b.csd_1 asc
) as _3
order by csd asc;

/*Aggregate all results to the project-level, because if a project matches with multiple CSDs, it'll appear in multiple rows*/

select *
into
aggregated_csd_project_level
from (
    select
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
        geometry,
        array_to_string(
            array_agg(
                nullif(
                    concat_ws(
                        ': ',
                        nullif(concat(csd), ''),
                        concat(round(100 * proportion_in_csd, 0), '%')
                    ),
                    ''
                )
            ),
            ' | '
        ) as csd
    --geometry_webmercator 
    from
        (select * from aggregated_csd_longform order by csd asc) as a
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
) as x;

-- this is a bit fragile - if remaining unassigned projects overlapped with multiple, this would have undesired behavior
-- quick fix on 3/29/23 to fix 43 records not matching. Verified via manual querying that this has desired outcome
update aggregated_csd_longform a
set
    csd = b.schooldist,
    proportion_in_csd = 1,
    units_net_in_csd = a.units_net
from dcp_school_districts as b
where
    a.csd is null
    and not st_isempty(a.geometry)
    and st_intersects(a.geometry, b.geometry);

/*
	Output final CSD-based KPDB. This is not at the project-level, but rather the project & CSD-level. It also omits Complete DOB jobs,
  	as these jobs should not be included in the forward-looking KPDB pipeline.


  	EP update 2021 - we now include completed DOB jobs in KPDB and SCA allocations

*/

select *
into
longform_csd_output
from (
    select * from aggregated_csd_longform
    -- where not (source = 'DOB' and status in('DOB 5. Completed Construction'))
    order by
        source asc,
        record_id asc,
        record_name asc,
        status asc
) as x;


-- select cdb_cartodbfytable('capitalplanning','longform_csd_output'); -- not necessary to run next script
