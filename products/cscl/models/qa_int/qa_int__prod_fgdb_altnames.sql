{{
  config(
    materialized='table'
  )
}}

-- Add _altnames_key to production fgdb_altnames output for QA comparison.
-- gdb_altnames has no natural row id - compare_gdb.py already treats the full
-- column tuple as row identity (lion_outputs.csv key_columns:
-- PDir|PType|SName|SType|SDir|Street|Join_ID) - so hash those same columns here
-- for a stable primary key dbt's audit_helper comparison can use.

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "fgdb_altnames"
) -%}

SELECT
    pdir,
    ptype,
    sname,
    stype,
    sdir,
    street,
    join_id,
    md5(concat_ws('|', pdir, ptype, sname, stype, sdir, street, join_id)) AS _altnames_key
FROM {{ prod_relation }}
