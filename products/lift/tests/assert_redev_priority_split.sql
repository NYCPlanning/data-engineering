{{
    config(
        meta = {
            'description': '''
                lift_supplemented_map splits redev_priority into redev_target_quarter and
                redev_horizon by pattern (LIFT-02). A value that looks like a quarter or a
                horizon but uses a new spelling, like "Q3-2027" or "Mid-Term", would otherwise
                land in redev_status and drop out of the quarter and horizon filters unnoticed.
            ''',
            'next_steps': '''
                Widen the pattern in lift_supplemented_map for the new spelling, or treat the
                value as a status if it is not a quarter or horizon.
            '''
        }
    )
}}

SELECT
    bbl,
    redev_priority
FROM {{ ref('lift_supplemented_map') }}
WHERE
    (REGEXP_MATCHES(LOWER(redev_priority), '^q\s*[1-4]') AND redev_target_quarter IS NULL)
    OR (REGEXP_MATCHES(LOWER(redev_priority), 'term') AND redev_horizon IS NULL)
