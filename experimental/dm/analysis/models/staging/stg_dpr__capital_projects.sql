-- one row per project. Phase dates are month and year, as MM/YYYY, read as the first of the month.
{% set phase_dates = [
    'DesignStart', 'DesignProjectedCompletion', 'DesignAdjustedCompletion', 'DesignActualCompletion',
    'ProcurementStart', 'ProcurementProjectedCompletion', 'ProcurementAdjustedCompletion', 'ProcurementActualCompletion',
    'ConstructionStart', 'ConstructionProjectedCompletion', 'ConstructionAdjustedCompletion', 'ConstructionActualCompletion',
] %}
SELECT
    "TrackerID" AS tracker_id,
    "FMSID" AS fms_id,
    "ContractID" AS contract_id,
    "Title" AS title,
    "Summary" AS summary,
    "CurrentPhase" AS current_phase,
    "DesignPercentComplete" AS design_percent_complete,
    "ProcurementPercentComplete" AS procurement_percent_complete,
    "ConstructionPercentComplete" AS construction_percent_complete,
    {%- for column in phase_dates %}
    strptime(nullif("{{ column }}", ''), '%m/%Y')::DATE AS {{ modules.re.sub('(?<!^)(?=[A-Z])', '_', column).lower() }},
    {%- endfor %}
    "TotalFunding" AS total_funding,
    "FundingSources"."FundingSource" AS funding_sources,
    "ProjectLiaison" AS project_liaison,
    strptime("LastUpdated", '%m/%d/%Y')::DATE AS last_updated,
    "Attachment" AS attachment_url,
    "VitalParksUnder400K" AS is_vital_parks_under_400k,
    "ProjectUpdate" AS project_update,
    "Boroughs"."Borough" AS boroughs
FROM {{ source('sources', 'dpr_capitalprojects') }}
