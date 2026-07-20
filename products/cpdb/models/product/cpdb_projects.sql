SELECT
    {{ dbt_utils.star(
        from=ref('cpdb_projects_geom'),
        except=["geom"])
    }}
FROM {{ ref('cpdb_projects_geom') }}
