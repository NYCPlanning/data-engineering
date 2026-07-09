{{
  config(
    materialized='table'
  )
}}

-- Normalize gdb_node column names from uppercase to lowercase for QA comparison
-- Build table intentionally uses uppercase quoted columns to match FGDB export schema

SELECT
    "NODEID" AS nodeid,
    "GLOBALID" AS globalid,
    "VIntersect" AS vintersect
FROM {{ ref('gdb_node') }}
