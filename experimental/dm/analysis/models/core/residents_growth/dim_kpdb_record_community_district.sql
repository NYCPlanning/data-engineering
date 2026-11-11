SELECT * FROM {{ ref('stg_kpdb__cd_records') }}
ORDER BY record_id, borocd
