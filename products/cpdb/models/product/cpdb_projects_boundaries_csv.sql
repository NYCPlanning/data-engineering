-- cpdb_projects_boundaries without geometry, for CSV export. Postgres writes booleans to CSV as
-- t and f, so the flags are cast to text to read true and false.
SELECT
    {{ dbt_utils.star(
        from=ref('cpdb_projects_boundaries'),
        except=['multi_boro', 'boros', 'boro_src', 'cd', 'multi_cd', 'cds', 'cd_src', 'citywide', 'geom'],
    ) }},
    multi_boro::text AS multi_boro,
    boros,
    boro_src,
    cd,
    multi_cd::text AS multi_cd,
    cds,
    cd_src,
    citywide::text AS citywide
FROM {{ ref('cpdb_projects_boundaries') }}
