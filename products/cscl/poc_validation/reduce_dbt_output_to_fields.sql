---- generates a long table of all field diffs by atomic id and borough
--
-- atomicid |field_name                   |prod_value|new_value|borocode|
-- ---------+-----------------------------+----------+---------+--------+
-- 000500093|patrol_borough               |"MS"      |"BS"     |1       |
-- 000500093|police_patrol_borough_command|"1"       |"4"      |1       |

WITH column_metadata AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE
        table_schema = 'ar_thinlion'
        AND table_name = 'qa__thinlion_brooklyn_comparison'
        AND column_name NOT LIKE 'dbt_%'
        AND column_name != 'atomicid'
    ORDER BY ordinal_position
),
in_prod AS (
    SELECT * FROM ar_thinlion.qa__thinlion_brooklyn_comparison
    WHERE dbt_audit_in_a = true
),
in_new AS (
    SELECT * FROM ar_thinlion.qa__thinlion_brooklyn_comparison
    WHERE dbt_audit_in_b = true
),
comparison AS (
    SELECT
        COALESCE(p.atomicid, n.atomicid) AS atomicid,
        p.atomicid AS prod_atomicid,
        n.atomicid AS new_atomicid,
        TO_JSONB(p.*) AS prod_row,
        TO_JSONB(n.*) AS new_row
    FROM in_prod AS p
    FULL OUTER JOIN in_new AS n ON p.atomicid = n.atomicid
    WHERE p.dbt_audit_row_status != 'identical' OR n.dbt_audit_row_status != 'identical'
),
field_diffs AS (
    SELECT
        atomicid,
        key AS field_name,
        prod_row -> key AS prod_value,
        new_row -> key AS new_value
    FROM comparison,
        LATERAL JSONB_OBJECT_KEYS(prod_row) AS key
    WHERE
        key NOT LIKE 'dbt_%'
        AND key != 'atomicid'
        AND (prod_row -> key IS DISTINCT FROM new_row -> key)
)
SELECT * FROM field_diffs
ORDER BY atomicid, field_name;
