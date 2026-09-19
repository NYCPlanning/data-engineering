{{ config(materialized='table') }}

-- Add _altnames_key to gdb_altnames for QA comparison. See qa_int__prod_fgdb_altnames
-- for why: no natural row id, so hash the same full column tuple compare_gdb.py
-- already treats as this layer's row identity.

SELECT
    "PDir" AS pdir,
    "PType" AS ptype,
    "SName" AS sname,
    "SType" AS stype,
    "SDir" AS sdir,
    "Street" AS street,
    "Join_ID" AS join_id,
    md5(concat_ws('|', "PDir", "PType", "SName", "SType", "SDir", "Street", "Join_ID")) AS _altnames_key
FROM {{ ref('gdb_altnames') }}
