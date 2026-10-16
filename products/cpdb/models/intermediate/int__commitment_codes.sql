SELECT DISTINCT
    trim(object) AS commitment_code,
    trim(object_name) AS name
FROM {{ ref('stg__fisa_capitalcommitments') }}
