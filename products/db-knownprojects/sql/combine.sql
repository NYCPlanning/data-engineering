/*
DESCRIPTION:
    Combines input data into a single table with a shared schema. For several 
    data sources, this involves joining on BBL with dcp_mappluto_wi to get lot-level
    polygon geometry. For some input datasets, a rows get collapsed to the level of
    a project. In these cases, the unique IDs for input data rows get stored in 
    the field record_id_input as an array. If no collapsing was necessary (i.e. 
    each project a single record in the a given source data table), the record_id_input
    array only contains the record_id. In cases where there is no unique project-level
    ID, the record_id gets assigned from a hash of the uids in record_id_input.
    This script also includes standardization of statuses, initial phasing assumptions,
    and calls to various string parsing functions to set flags.
INPUTS: 
    dcp_mappluto_wi
    dcp_application
    edc_projects
    edc_dcp_inputs
    dcp_planner_added
    dcp_n_study_future
    dcp_rezoning
    dcp_n_study_projected
    dcp_n_study
    esd_projects
    hpd_pc
    hpd_rfp
    dcp_housing_poly
OUTPUTS: 
    combined
*/
DROP TABLE IF EXISTS combined;
WITH 
_dcp_application as (
    SELECT
        source,
        record_id,
        array_append(array[]::text[], a.record_id::text) as record_id_input,
        record_name,
        status,
        NULL as type,
        units_gross,
        dcp_certifiedreferred as date,  
        'Certified Referred' as date_type,
        0 as prop_within_5_years,
        (CASE 
            WHEN status = 'Record Closed'
            THEN 0 ELSE 1
        END) as prop_5_to_10_years,
        0 as prop_after_10_years, 
      	0 as phasing_known,
        geom,
        flag_nycha(a::text) as nycha,
        flag_classb(a::text) as classb,
        flag_senior_housing(a::text) as senior_housing
    FROM dcp_application a
    WHERE flag_relevant=1
),
_edc_projects as (
    WITH
    geom_bbl as (
        SELECT 
            a.uid,
            st_union(b.wkb_geometry) as geom
        FROM(
            select uid,  UNNEST(string_to_array(coalesce(bbl, 'NA'), ';')) as bbl
            from edc_projects 
        ) a LEFT JOIN dcp_mappluto_wi b
        ON a.bbl = b.bbl::bigint::text
        GROUP BY a.uid
    ),
    geom_borough_block as (
        SELECT 
            a.uid, 
            st_union(b.wkb_geometry) as geom
        FROM edc_projects a
        LEFT JOIN dcp_mappluto_wi b
        ON a.block = b.block::text 
        AND a.borough_code = b.borocode::text
        GROUP BY a.uid
    ),
    geom_edc_dcp_inputs as (
        SELECT 
            a.uid,
            b.geometry as geom 
        FROM edc_projects a
        LEFT JOIN edc_dcp_inputs b
        ON a.edc_id::numeric = b.edc_id::numeric
    ),
    geom_consolidated as (
        SELECT a.uid,coalesce(a.geom, b.geom) as geom
        FROM (
            SELECT a.uid,coalesce(a.geom, b.geom) as geom
            FROM geom_edc_dcp_inputs a LEFT JOIN geom_bbl b 
            ON a.uid = b.uid
            ) a LEFT JOIN geom_borough_block b 
        ON a.uid = b.uid
    )
    SELECT 
        'EDC Projected Projects' as source,
        a.uid as record_id,
        array_append(array[]::text[], a.uid) as record_id_input,
        project_name as record_name,
        'Potential' as status,
        NULL as type,
        total_units::numeric as units_gross,
        build_year as date,
        'Build Year' as date_type,
          
        -- phasing
        (CASE 
            WHEN build_year::numeric <= date_part('year', CURRENT_DATE)+5 
            THEN 1 ELSE 0 
        END) as prop_within_5_years,
        (CASE 
            WHEN build_year::numeric > date_part('year', CURRENT_DATE)+5 
            AND build_year::numeric <= date_part('year', CURRENT_DATE)+10 
            THEN 1 ELSE 0 
        END)as prop_5_to_10_years,
        (CASE 
            WHEN build_year::numeric > date_part('year', CURRENT_DATE)+10 
            THEN 1 ELSE 0 
        END) as prop_after_10_years,
        1 as phasing_known,
        b.geom as geom,
        flag_nycha(a::text) as nycha,
        flag_classb(a::text) as classb,
        flag_senior_housing(a::text) as senior_housing
    FROM edc_projects a
    LEFT JOIN geom_consolidated b
    ON a.uid = b.uid
),
_dcp_planneradded as (
    SELECT 
        'DCP Planner-Added Projects' as source,
        uid as record_id,
        array_append(array[]::text[], a.uid) as record_id_input,
        project_na as record_name,
        NULL as status,
        NULL as type,
        total_unit::numeric as units_gross,
        NULL as date,
        NULL as date_type,
        portion_bu::numeric as prop_within_5_years,
        portion_1::numeric as prop_5_to_10_years,
        portion_2::numeric as prop_after_10_years,
        1 as phasing_known,
        geometry as geom,
        flag_nycha(a::text) as nycha,
        flag_classb(a::text) as classb,
        flag_senior_housing(a::text) as senior_housing
    FROM dcp_planneradded a
),
_dcp_n_study_future as (
    SELECT
        'Future Neighborhood Studies' as source, 
        uid as record_id,
        array_append(array[]::text[], uid) as record_id_input,
        project_id||' '||'Future Rezoning Development' as record_name,
        'Potential' as status,
        'Future Rezoning' as type,
        total_unit::numeric as units_gross,
        adoption as date,
        'Effective Date' as date_type,
        within5::numeric as prop_within_5_years,
        "5to10"::numeric as prop_5_to_10_years,
        after10::numeric as prop_after_10_years, 
        0 as phasing_known, 
        geometry as geom,
        NULL::INTEGER as nycha,
        NULL::INTEGER as classb,
        NULL::INTEGER as senior_housing
    FROM dcp_n_study_future
),
_dcp_n_study_projected as (
    SELECT 
        'Neighborhood Study Projected Development Sites' as source,
        uid as record_id,
        array_append(array[]::text[], a.uid) as record_id_input,
        REPLACE(study, ' Projected Development Sites', '') as record_name,
        'Potential' as status,
        'Past Rezoning' AS type,
        total_unit::numeric as units_gross,
        -- TO_CHAR(TO_DATE(effective_date, 'MM/DD/YYYY'), 'YYYY/MM/DD') as date,
        adoption as date,
        'Effective Date' as date_type,
        within5::numeric as prop_within_5_years,
        "5to10"::numeric as prop_5_to_10_years,
        after10::numeric as prop_after_10_years, 
        1 as phasing_known,
        geometry as geom,
        NULL::INTEGER as nycha,
        NULL::INTEGER as classb,
        NULL::INTEGER as senior_housing
    FROM dcp_n_study_projected a
),
_dcp_n_study as (
    SELECT 
        source,
        uid::text as record_id,
        array_append(array[]::text[], uid::text) as record_id_input,
        project_na as record_name,
        'Potential' as status,
        NULL::text as type,
        total_unit::numeric as units_gross,
        NULL::text as date,
        NULL::text as date_type,
        within5::numeric as prop_within_5_years,
        "5to10"::numeric as prop_5_to_10_years,
        after10::numeric as prop_after_10_years, 
        1 as phasing_known,
        geometry as geom,
        NULL::INTEGER as nycha,
        NULL::INTEGER as classb,
        NULL::INTEGER as senior_housing
    FROM dcp_n_study
),
_esd_projects as (
    SELECT  
        'Empire State Development Projected Projects' as source,
        md5(array_to_string(array_agg(a.uid), '')) as record_id,
        array_agg(a.uid) as record_id_input,
        a.project_name as record_name,
        'Potential' as status,
        NULL as type,
        total_units::numeric as units_gross,
        NULL as date,
        NULL as date_type,
        NULL::numeric as prop_within_5_years,
        NULL::numeric as prop_5_to_10_years,
        1 as prop_after_10_years, 
        0 as phasing_known,
        ST_UNION(b.wkb_geometry) as geom,
        flag_nycha(array_agg(row_to_json(a))::text) as nycha,
        flag_classb(array_agg(row_to_json(a))::text) as classb,
        flag_senior_housing(array_agg(row_to_json(a))::text) as senior_housing
    FROM esd_projects a
    LEFT JOIN dcp_mappluto_wi b
    ON a.bbl::numeric = b.bbl::numeric
    GROUP BY project_name, total_units
),
_hpd_pc as (
    SELECT 
        'HPD Projected Closings' as source,
        a.uid as record_id,
        array_append(array[]::text[], a.uid) as record_id_input,
        house_number||' '||street_name as record_name,
        'HPD 3: Projected Closing' as status,
        NULL as type,
        ((min_of_projected_units::INTEGER + 
            max_of_projected_units::INTEGER)/2
        )::integer as units_gross,

        -- dates
        projected_fiscal_year_range as date,
        'Projected Fiscal Year Range' as date_type,

        -- phasing
        (CASE 
        WHEN date_part('year', age(to_date((CONCAT(RIGHT(projected_fiscal_year_range,4)::numeric+3, '-06-30')),'YYYY-MM-DD'),CURRENT_DATE)) <= 5 
        THEN 1 ELSE 0 
        END)::numeric as prop_within_5_years,

        (CASE 
        WHEN date_part('year',age(to_date((CONCAT(RIGHT(projected_fiscal_year_range,4)::numeric+3,'-06-30')),'YYYY-MM-DD'),CURRENT_DATE)) > 5 
        AND date_part('year',age(to_date((CONCAT(RIGHT(projected_fiscal_year_range,4)::numeric+3,'-06-30')),'YYYY-MM-DD'),CURRENT_DATE)) <= 10 THEN 1 
        ELSE 0 
        END)::numeric as prop_5_to_10_years,

        (CASE 
        WHEN date_part('year',age(to_date((CONCAT(RIGHT(projected_fiscal_year_range,4)::numeric+3,'-06-30')),'YYYY-MM-DD'),CURRENT_DATE)) > 10 
        THEN 1 ELSE 0 
        END)::numeric as prop_after_10_years,
        1 as phasing_known,
        b.wkb_geometry as geom,
        flag_nycha(a::text) as nycha,
        flag_classb(a::text) as classb,
        flag_senior_housing(a::text) as senior_housing
    FROM hpd_pc a
    LEFT JOIN dcp_mappluto_wi b
    ON a.bbl::numeric = b.bbl::numeric
),
_hpd_rfp as (
    SELECT 
        'HPD RFPs' AS source,
        md5(array_to_string(array_agg(a.uid), '')) AS record_id,
        array_agg(a.uid) AS record_id_input,
        request_for_proposals_name AS record_name,
        (CASE 
            WHEN designated = 'Y' AND closed = 'Y' 
                THEN 'HPD 4: Financing Closed'
            WHEN designated = 'Y' AND closed = 'N' 
                THEN 'HPD 2: RFP Designated'
            WHEN designated = 'N' AND closed = 'N' 
                THEN 'HPD 1: RFP Issued'
        END) AS status,
        NULL AS type,
        (CASE 
            WHEN est_units ~* '-' THEN NULL 
            ELSE REPLACE(est_units, ',', '') 
        END)::integer AS units_gross,
        (CASE 
            WHEN closed_date = '-' THEN NULL 
            ELSE TO_CHAR(closed_date::date, 'YYYY/MM') 
        END) AS date,
        'Month Closed' AS date_type,
        1 as prop_within_5_years,
        0 as prop_5_to_10_years,
        0 as prop_after_10_years,
        1 as phasing_known,
        st_union(b.wkb_geometry) AS geom,
        flag_nycha(array_agg(row_to_json(a))::text) as nycha,
        flag_classb(array_agg(row_to_json(a))::text) as classb,
        flag_senior_housing(array_agg(row_to_json(a))::text) as senior_housing
    FROM hpd_rfp a
    LEFT JOIN dcp_mappluto_wi b
    ON a.bbl::numeric = b.bbl::numeric
    GROUP BY request_for_proposals_name, designated, 
    closed, est_units, closed_date, likely_to_be_built_by_2025
),
/* Housing data, as mapped in _sql/dcp_housing.sql
    NOTE: this still includes contextual class B records */
_dcp_housing AS (
    SELECT
        source,
        record_id::text,
        array_append(array[]::text[], record_id::text) as record_id_input,
        record_name,
        status,
        type,
        units_gross::integer,
        date,
        date_type,
        prop_within_5_years,
        prop_5_to_10_years,
        prop_after_10_years,
        phasing_known,
        geom,
        nycha,
        classb,
        senior_housing,
        inactive,
        no_classa
    FROM dcp_housing_poly
)
SELECT
    *,
    NULL::text as phasing_rationale
INTO combined
FROM(
    SELECT 
        *, 
        NULL::numeric as inactive,
        NULL::text as no_classa
    FROM (
        SELECT * FROM _dcp_application UNION
        SELECT * FROM _edc_projects UNION
        SELECT * FROM _dcp_planneradded UNION
        SELECT * FROM _dcp_n_study UNION
        SELECT * FROM _dcp_n_study_future UNION
        SELECT * FROM _dcp_n_study_projected UNION
        SELECT * FROM _esd_projects UNION
        SELECT * FROM _hpd_pc UNION
        SELECT * FROM _hpd_rfp
    ) a
    UNION SELECT * FROM _dcp_housing
) a
WHERE record_id NOT IN (
    SELECT record_id FROM corrections_main
    WHERE field = 'remove'
);

CREATE INDEX combined_geom_idx ON combined USING GIST(geom);