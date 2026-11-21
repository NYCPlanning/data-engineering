-- one row per piece of segment line and project. DDC doesn't document its shapefile's cut-off column names, so only
-- the design and construction dates are renamed, from their names and the order of their values. The contact
-- person and phone are left out.
SELECT
    projectid AS ddc_project_id,
    segmentid AS segment_id,
    active = 'Y' AS is_active,
    descriptio AS description,
    phase,
    status,
    sponsor,
    managing AS managing_agency,
    type AS project_types,
    project_ca,
    division,
    unit,
    design_sta::DATE AS design_start,
    design_com::DATE AS design_completion,
    constr_sta::DATE AS construction_start,
    constr_com::DATE AS construction_completion,
    projected_::DATE AS projected_,
    projected1::DATE AS projected1,
    actual_sta::DATE AS actual_sta,
    estatcompt,
    bmsecondar,
    wkb_geometry AS geom
FROM {{ source('sources', 'ddc_capitalprojects_infrastructure') }}
