-- TODO: implement node_stname non-spatial layer for LION GDB
-- Schema: NODEID (int), STNAME (str)
-- Requires export.py support for non-spatial GDB tables
{{ config(materialized='table') }}
SELECT 1 AS stub
WHERE false
