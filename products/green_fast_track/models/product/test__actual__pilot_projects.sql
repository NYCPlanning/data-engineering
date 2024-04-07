WITH gft_bbls AS (
    SELECT * FROM {{ ref('green_fast_track_bbls') }}
),

expected_pilot_projects AS (
    SELECT * FROM {{ ref('test__expected_pilot_projects') }}
),

final AS (
    SELECT
        expected_pilot_projects.project_label,
        gft_bbls.*
    FROM gft_bbls INNER JOIN expected_pilot_projects
        ON gft_bbls.bbl = expected_pilot_projects.bbl
)

SELECT * FROM final ORDER BY project_label ASC, bbl ASC
