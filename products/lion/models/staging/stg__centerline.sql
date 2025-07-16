{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}

SELECT
    {{ dbt_utils.star(from=source("recipe_sources", "dcp_cscl_centerline"), except=['geom']) }},
    st_linemerge(geom) AS geom, -- TODO - any reason to not do this here?
    st_lineinterpolatepoint(st_linemerge(geom), 0.5) AS midpoint -- ditto above?
FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
WHERE
    NOT (rwjurisdiction = '3' AND status <> '2')
    AND rw_type <> 8
