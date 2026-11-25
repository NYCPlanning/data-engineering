SELECT * FROM {{ ref('int_major_felony_complaint_spine') }}
ORDER BY complaint_id
