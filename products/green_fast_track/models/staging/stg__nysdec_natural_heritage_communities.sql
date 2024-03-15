{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_natural_heritage_communities"), left_by="wkb_geometry") }}
)

SELECT * FROM clipped_to_nyc
