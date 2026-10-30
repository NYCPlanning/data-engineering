{% set counts = {
    'pop1': 'population',
    'popinhh_1': 'population_in_households',
    'hunits': 'housing_units',
    'ochu_1': 'occupied_housing_units',
    'vachus': 'vacant_housing_units',
} %}
SELECT
    borocd,
    {%- for variable, name in counts.items() %}
    {%- for year in [2010, 2020] %}
    max(value) FILTER (WHERE census_year = {{ year }} AND pff_variable = '{{ variable }}')
        AS {{ name }}_{{ year }}_census,
    {%- endfor %}
    {%- endfor %}
    -- everyone not in a household lives in group quarters (dorms, nursing homes, prisons, shelters)
    {%- for year in [2010, 2020] %}
    max(value) FILTER (WHERE census_year = {{ year }} AND pff_variable = 'pop1')
    - max(value) FILTER (WHERE census_year = {{ year }} AND pff_variable = 'popinhh_1')
        AS group_quarters_population_{{ year }}_census{{ "," if not loop.last }}
    {%- endfor %}
FROM {{ ref('stg_pff__decennial') }}
GROUP BY borocd
