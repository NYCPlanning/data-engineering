-- One column per function in the map's table, true where the facility has a dot, including emergency-only ones.
{%- set functions = {
    'Preliminary Treatment': 'preliminary_treatment',
    'Secondary Treatment': 'secondary_treatment',
    'Biological Nutrient Removal (BNR)': 'biological_nutrient_removal',
    'Chlorination': 'chlorination',
    'Dechlorination': 'dechlorination',
    'Sludge Dewatering': 'sludge_dewatering',
    'Sludge Pumping': 'sludge_pumping',
    'Docks': 'docks',
    'Gas to Grid / Food Waste': 'gas_to_grid_food_waste',
    'Cogen Engines': 'cogen_engines',
    'Title V Facility': 'title_v_facility',
    'CSO Facilities': 'cso_facilities',
} %}
SELECT
    facility.wastewater_facility_id,
{%- for function, column in functions.items() %}
    coalesce(bool_or(function.function = '{{ function }}'), false) AS has_{{ column }},
{%- endfor %}
    string_agg(function.function, ', ') FILTER (WHERE function.is_emergency_only) AS emergency_only_functions
FROM {{ ref('int_wastewater_facility_spine') }} AS facility
LEFT JOIN {{ ref('stg_dep__wastewater_facility_functions') }} AS function
    ON facility.wastewater_facility_id = function.wastewater_facility_id
GROUP BY facility.wastewater_facility_id
