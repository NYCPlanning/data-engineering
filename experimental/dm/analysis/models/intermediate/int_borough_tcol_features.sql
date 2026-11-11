-- TCOL rates by borough (Table 9) and by borough and race and ethnicity (Table 10), one row per borough
{%- set races = {'Hispanic': 'hispanic', 'Black': 'black', 'Asian/PI': 'asian_pi', 'White': 'white'} %}
SELECT
    rates.borough AS boro_name,
    rates.pct_not_meeting AS tcol_pct_not_meeting,
    rates.pct_adults_no_children_not_meeting AS tcol_pct_adults_no_children_not_meeting,
    rates.pct_families_with_children_not_meeting AS tcol_pct_families_with_children_not_meeting,
    rates.avg_resource_gap AS tcol_avg_resource_gap,
{%- for race, column in races.items() %}
    max(race.pct_not_meeting) FILTER (WHERE race.race_ethnicity = '{{ race }}') AS tcol_pct_not_meeting_{{ column }}{{ "," if not loop.last }}
{%- endfor %}
FROM {{ ref('stg_tcol__borough_rates') }} AS rates
LEFT JOIN {{ ref('stg_tcol__borough_race_rates') }} AS race ON rates.borough = race.borough
WHERE rates.borough <> 'NYC Overall'
GROUP BY ALL
