SELECT
    record_id,
    nta AS nta2020,
    {{ kpdb_record_columns('nta') }}
FROM {{ source('sources', 'kpdb_longform_nta_output') }}
