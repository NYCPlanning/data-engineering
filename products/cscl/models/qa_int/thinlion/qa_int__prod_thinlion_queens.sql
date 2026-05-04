-- Add atomic ID to production thinlion output for QA comparison
-- Atomic ID is used as the primary key for dbt audit_helper comparisons

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "thinlion_queens"
) -%}

SELECT
  *,
  COALESCE(borough, '') ||
  LPAD(COALESCE(TRIM(censustract_2020_basic::text), ''), 4, '0') ||
  LPAD(COALESCE(TRIM(censustract_2020_suffix::text), ''), 2, '0') ||
  COALESCE(dynamic_block, '') AS atomicid
FROM {{ prod_relation }}
