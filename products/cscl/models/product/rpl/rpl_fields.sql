SELECT
    {{ select_fields_from_seed('text_formatting__rpl') }}
FROM {{ ref("int__rpl") }}
ORDER BY rpl_id ASC
