-- FISA labels each release by the fiscal year it was released in and its month: CPS in
-- September, CPJ in January, CPA in April. A cycle can have more than one delivery.
WITH releases AS (
    SELECT DISTINCT
        version AS fisa_version,
        cycle_fy,
        cycle_name
    FROM {{ ref('fisa_typc_names') }}
)

SELECT
    fisa_version,
    cycle_fy,
    cycle_name,
    row_number() OVER (
        ORDER BY
            cycle_fy,
            CASE cycle_name WHEN 'CPS FP' THEN 1 WHEN 'CPJ FP' THEN 2 WHEN 'CPA FP' THEN 3 END,
            fisa_version
    ) AS release_order
FROM releases
