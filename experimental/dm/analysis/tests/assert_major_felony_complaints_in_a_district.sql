-- Warns rather than fails: complaints in no district still count citywide, and this lists them to triage.
{{ config(severity='warn') }}
SELECT
    complaint_id,
    boro_name,
    precinct,
    premises_type,
    geom
FROM {{ ref('fact_major_felony_complaint') }}
WHERE borocd IS NULL
