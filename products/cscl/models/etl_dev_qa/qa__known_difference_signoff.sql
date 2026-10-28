{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs']
  )
}}

-- One row per (known difference, output_group it touches): the reviewers the
-- signoff_matrix requires for that output_group, the difference's status from each,
-- and whether every required reviewer has agreed. qa__diffs_all joins on this to mark
-- each accounted-for row approved.

{{ known_difference_signoff() }}
