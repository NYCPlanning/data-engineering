-- poverty_metrics() counts part of the $150,000-$199,999 family income bracket as under the TCOL threshold, which
-- only works while the threshold falls inside it
SELECT median_amount
FROM {{ ref('stg_tcol__budget_components') }}
WHERE broad_family_type = 'Adults under 65, with children'
    AND component = 'Total Cost'
    AND NOT median_amount BETWEEN 150000 AND 200000
