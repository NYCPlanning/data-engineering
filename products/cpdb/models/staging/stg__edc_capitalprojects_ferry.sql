SELECT
    {{ dbt_utils.star(
        from=source('recipe_sources', 'edc_capitalprojects_ferry'),
        except=["geom"],
        quote_identifiers=False)
    }},
    ST_TRANSFORM(wkb_geometry, 2263) AS geom
FROM {{ source('recipe_sources', 'edc_capitalprojects_ferry') }}
