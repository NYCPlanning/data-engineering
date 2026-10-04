{{ config(materialized='table') }}

-- int__boundary__sd vs production_outputs.fgdb_nysd, which stores SD 10 once per
-- borough (33 rows, 32 districts) - dissolved per key before comparing.

{{ district_boundary_vs_prod(
    'int__boundary__sd',
    'qa__boundary__sd_validity',
    'fgdb_nysd',
    'schooldist',
    'sd',
    dissolve_prod=true
) }}
