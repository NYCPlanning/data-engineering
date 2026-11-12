SELECT
    record_id,
    cd::integer AS borocd,
    {{ kpdb_record_columns('cd') }}
FROM {{ source('sources', 'kpdb_longform_cd_output') }}
