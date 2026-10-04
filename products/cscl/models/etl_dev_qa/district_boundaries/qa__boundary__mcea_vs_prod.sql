{{ config(materialized='table') }}

-- int__boundary__mcea vs production_outputs.fgdb_nymcea, which stores one feature per
-- disjoint piece (122 features for 115 areas) - dissolved per key before comparing.

{{ district_boundary_vs_prod(
    'int__boundary__mcea',
    'qa__boundary__mcea_validity',
    'fgdb_nymcea',
    'mcea',
    'mcea',
    dissolve_prod=true
) }}
