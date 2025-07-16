{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH dcp_zoningdistricts AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningdistricts') }}
),

rename AS (
    SELECT
        ogc_fid,
        wkb_geometry AS geom,
        zonedist
    FROM dcp_zoningdistricts
),

validzones AS (
    SELECT
        CASE
            WHEN zonedist = any('{"BALL FIELD", "PLAYGROUND", "PUBLIC PLACE"}') THEN 'PARK'
            ELSE zonedist
        END AS zonedist,
        ST_MakeValid(geom) AS geom
    FROM rename
    WHERE ST_GeometryType(ST_MakeValid(geom)) = 'ST_MultiPolygon'
)

SELECT * FROM validzones
