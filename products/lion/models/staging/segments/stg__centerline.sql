{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['segmentid']} ]
) }}

SELECT
    {{ dbt_utils.star(from=source("recipe_sources", "dcp_cscl_centerline"), except=['geom']) }},
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND rw_type <> 8 AS include_in_geosupport_lion,
    (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AS include_in_bytes_lion
FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
