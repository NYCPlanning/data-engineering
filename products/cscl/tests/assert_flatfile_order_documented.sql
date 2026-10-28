-- Every flat file has an order rule, and every file that has to match prod's order says
-- what that order is, what the ETL spec says (citing its line, or saying it's not in the
-- spec) and whether they differ: the export doc's Record order section is generated from
-- these. Files other than flat files carry n_a. Reads only lion_outputs.

WITH outputs AS (
    SELECT
        file_id,
        output_group,
        order_rule,
        trim(coalesce(prod_order, '')) AS prod_order,
        trim(coalesce(spec_order, '')) AS spec_order,
        order_differs_from_spec
    FROM {{ ref('lion_outputs') }}
),

problems AS (
    SELECT
        file_id,
        CASE
            WHEN output_group != 'flat_files' AND order_rule != 'n_a'
                THEN 'not a flat file, so order_rule must be n_a'
            WHEN output_group = 'flat_files' AND order_rule NOT IN ('prod', 'none')
                THEN 'a flat file needs order_rule prod or none'
            WHEN order_rule = 'prod' AND prod_order = ''
                THEN 'order_rule prod needs prod_order'
            WHEN order_rule = 'prod' AND spec_order = ''
                THEN 'order_rule prod needs spec_order'
            WHEN
                order_rule = 'prod'
                AND NOT regexp_matches(spec_order, '\bL[0-9]+\b')
                AND NOT starts_with(spec_order, 'Not in the spec')
                THEN 'spec_order must cite a spec line (L123) or say "Not in the spec"'
            WHEN
                order_rule = 'prod'
                AND order_differs_from_spec NOT IN ('true', 'false', 'spec_silent')
                THEN 'order_rule prod needs order_differs_from_spec true, false or spec_silent'
            WHEN
                order_rule = 'prod'
                AND starts_with(spec_order, 'Not in the spec')
                AND order_differs_from_spec != 'spec_silent'
                THEN 'a file not in the spec is spec_silent'
            WHEN
                order_rule != 'prod'
                AND (prod_order != '' OR spec_order != '' OR order_differs_from_spec != 'n_a')
                THEN 'only an order_rule prod file has prod_order, spec_order and order_differs_from_spec'
        END AS problem
    FROM outputs
)

SELECT * FROM problems
WHERE problem IS NOT NULL
