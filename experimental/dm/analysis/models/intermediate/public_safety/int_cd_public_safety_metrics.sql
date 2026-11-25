SELECT
    spine.borocd,
    count(complaints.complaint_id) FILTER (WHERE year(complaints.occurred_date) = 2025) AS major_felony_complaints_2025
FROM {{ ref('int_cd_spine') }} AS spine
LEFT JOIN {{ ref('int_major_felony_complaint_spine') }} AS complaints ON spine.borocd = complaints.borocd
GROUP BY spine.borocd
