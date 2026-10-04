{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2010 census blocks that differ from prod, broken into viewing layers.

{{ district_boundary_outliers('cb2010', 'bctcb2010', 'fgdb_nycb2010', 'bctcb2010') }}
