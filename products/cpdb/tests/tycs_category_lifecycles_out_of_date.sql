-- tycs_category_lifecycles comes from the strategy published 2025-05-01. A later release
-- means the lifecycle mapping needs redoing from its "Life Cycle Category" page.
{{ config(severity='warn') }}

SELECT DISTINCT pub_date
FROM {{ ref('stg__omb_tycs') }}
WHERE pub_date > '2025-05-01'
