SELECT
    commitment_code,
    name
FROM {{ ref('int__commitment_codes') }}
