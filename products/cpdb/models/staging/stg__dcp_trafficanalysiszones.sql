SELECT
    {{ dbt_utils.star(
        from=source('recipe_sources', 'dcp_trafficanalysiszones'),
        except=["geom"],
        quote_identifiers=False)
    }},
    ST_TRANSFORM(ST_SETSRID(wkb_geometry, 4326), 2263) AS geom
FROM {{ source('recipe_sources', 'dcp_trafficanalysiszones') }}
