{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH dof_dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

validdtm AS (
    SELECT
        id AS dtm_id,
        bbl,
        ST_MAKEVALID(geom) AS geom
    FROM dof_dtm
    WHERE ST_GEOMETRYTYPE(ST_MAKEVALID(geom)) = 'ST_MultiPolygon'
)

SELECT * FROM validdtm
