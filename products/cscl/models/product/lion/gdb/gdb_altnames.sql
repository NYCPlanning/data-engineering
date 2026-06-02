-- TODO: implement altnames non-spatial layer for LION GDB
-- Schema: PDir, PType, SName, SType, SDir, Street, Join_ID (all str)
-- Requires export.py support for non-spatial GDB tables
{{ config(materialized='table') }}
SELECT 1 AS stub
WHERE false
