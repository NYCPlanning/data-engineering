WITH jobs_bis AS (
    SELECT * FROM {{ ref("stg__jobs_bis") }}
),

parking_spaces AS (
    SELECT * FROM {{ source("dob_jobapplications_parkingspaces")}}
)

SELECT 

FROM jobs_bis
LEFT JOIN parking_spaces AS ps ON jobs_bis.jobnumber = ps.jobnumber
