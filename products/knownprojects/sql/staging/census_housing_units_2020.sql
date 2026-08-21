{{ config(materialized='table', tags=['cpp']) }}

/*
2020 Census housing unit counts, the baseline every CPP growth figure builds on.

Only the geographies KPDB aggregates to are kept. dcp_censusdata reports one row
per geography per geotype, so no aggregation is needed here.
*/

SELECT
    geotype AS geography_type,
    geoid::text AS geography_id,
    coalesce(hunits::int, 0) AS units_2020
FROM {{ env_var('BUILD_ENGINE_SCHEMA') }}.dcp_censusdata -- noqa: PRS, TMP
WHERE geotype IN ('NTA2020', 'CD')
