-- lift_supplemented plus each bbl's PLUTO lot geometry, for the gdb export. Kept separate
-- so lift_supplemented.csv doesn't carry a geometry column.
-- Unlike lift_supplemented, this model isn't held to DCAS's field spec: columns are
-- retyped so map tools (CARTO Builder) can filter and style on them. Flags become booleans,
-- with NULL as "no" per the data dictionary's Y/NULL spec (lur_underwater's entry lists
-- wetland types instead, but the data only has 'Y'). Dollar text like ' $(80,200,000)'
-- becomes numeric for the columns the dictionary types as numbers. cp_remediation stays
-- text, trimmed: the dictionary describes it as a short description, and it holds values
-- like 'Requested' alongside dollar amounts.
{%- set dollar_columns = ['rlv_amount', 'siteprep_costs'] %}
{#- Y/NULL flags, with the label each gets in dcas_flags. #}
{%- set yn_flags = {
    'lur_underwater': 'Underwater',
    'lur_waterfront': 'Waterfront',
    'lur_urbanrenewalsite': 'Urban renewal site',
    'lur_sliverlot': 'Sliver lot',
    'no_street_frontage': 'No street frontage',
    'lur_accessway': 'Accessway',
    'lur_cemetery': 'Cemetery',
    'lur_commonopenspace': 'Common open space',
    'lur_bedofstreet': 'Bed of street',
} %}
{#- Public Sites categories typed by hand, so the same value arrives in several spellings
    ('New Construction ', 'Co-location' vs 'Co-Location'). Each is collapsed to its most
    common spelling so map widgets don't split one category into several bars. #}
{%- set category_columns = ['redev_strategy', 'redev_priority'] %}
{%- set squish = "REGEXP_REPLACE(TRIM({}), '\\s+', ' ', 'g')" %}
WITH lift AS (
    SELECT * FROM {{ ref('lift_supplemented') }}
),

pluto AS (
    SELECT bbl, geom FROM {{ ref('stg__pluto') }}
),

ahft_districts AS (
    SELECT borocd FROM {{ ref('stg__dcp_housing_ahft') }}
),

tracts AS (
    SELECT boroct2020, cdta2020, cdtaname FROM {{ ref('stg__ct2020') }}
),

districts AS (
    SELECT borocd, community_district FROM {{ ref('stg__dcp_cdboundaries') }}
),
{% for col in category_columns %}
{{ col }}_spellings AS (
    SELECT
        LOWER(spelling) AS spelling_key,
        FIRST(spelling ORDER BY n DESC, spelling) AS canonical
    FROM (
        SELECT {{ squish.format(col) }} AS spelling, COUNT(*) AS n
        FROM lift
        WHERE {{ col }} IS NOT NULL
        GROUP BY 1
    )
    GROUP BY 1
){{ "," if not loop.last }}
{% endfor %}

SELECT
    lift.* REPLACE (
        CAST(lift.cd AS VARCHAR) AS cd,
        CAST(lift.council AS VARCHAR) AS council,
        TRY_CAST(lift.unitpotential AS INTEGER) AS unitpotential,
        TRIM(lift.cp_remediation) AS cp_remediation,
        {%- for col in dollar_columns %}
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(TRIM(lift.{{ col }}), '$', ''), ',', ''), '(', '-'), ')', '')
            AS DOUBLE
        ) AS {{ col }},
        {%- endfor %}
        {%- for flag in yn_flags %}
        COALESCE(lift.{{ flag }} = 'Y', FALSE) AS {{ flag }},
        {%- endfor %}
        -- Both depend on a PLUTO match, so a bbl without one is unknown, not "no".
        -- pfirm15_flag comes from DCAS's PLUTO block, laa from our join to PLUTO geometry.
        CASE WHEN lift.pluto_bbl IS NOT NULL THEN COALESCE(lift.pfirm15_flag = 1, FALSE) END AS pfirm15_flag,
        CASE WHEN pluto.geom IS NOT NULL THEN COALESCE(lift.laa = 'Y', FALSE) END AS laa,
        -- AHFT only ranks the 59 community districts, so a lot outside them is unranked, not "no".
        CASE WHEN ahft_districts.borocd IS NOT NULL THEN COALESCE(lift.ahft = 'Y', FALSE) END AS ahft,
        {%- for col in category_columns %}
        {{ col }}_spellings.canonical AS {{ col }}{{ "," if not loop.last }}
        {%- endfor %}
    ),
    CASE lift.boro
        WHEN 1 THEN 'Manhattan'
        WHEN 2 THEN 'Bronx'
        WHEN 3 THEN 'Brooklyn'
        WHEN 4 THEN 'Queens'
        WHEN 5 THEN 'Staten Island'
    END AS borough,
    -- zzz_lift_data_zzz holds the Public Sites site name (see README Limitations).
    lift.zzz_lift_data_zzz IS NOT NULL AS is_lift_site,
    lift.zzz_lift_data_zzz AS site_name,
    districts.community_district,
    tracts.cdta2020,
    tracts.cdtaname,
    -- One readable list of the flags a lot carries, for popups; null when it has none.
    NULLIF(CONCAT_WS(', ',
        {%- for flag, label in yn_flags.items() %}
        CASE WHEN lift.{{ flag }} = 'Y' THEN '{{ label }}' END{{ "," if not loop.last }}
        {%- endfor %}
    ), '') AS dcas_flags,
    -- IPIS agency use records: one 'O' (owned by the City) or 'L' (leased by the City) code
    -- per agency in agencys, so a lot can be both.
    CASE
        WHEN lift.ownedleased IS NULL THEN 'No agency use record'
        WHEN lift.ownedleased LIKE '%O%' AND lift.ownedleased LIKE '%L%' THEN 'Owned and leased'
        WHEN lift.ownedleased LIKE '%L%' THEN 'Leased'
        WHEN lift.ownedleased LIKE '%O%' THEN 'Owned'
    END AS owned_leased,
    ARRAY_TO_STRING(LIST_TRANSFORM(
        RANGE(1, LEN(STRING_SPLIT(lift.agencys, ',')) + 1),
        i -> TRIM(STRING_SPLIT(lift.agencys, ',')[i]) || ': '
            || CASE TRIM(STRING_SPLIT(lift.ownedleased, ',')[i]) WHEN 'O' THEN 'owned' WHEN 'L' THEN 'leased' END
    ), ', ') AS agency_owned_leased,
    pluto.geom
FROM lift
LEFT JOIN pluto ON lift.bbl = pluto.bbl
LEFT JOIN ahft_districts ON lift.cd = ahft_districts.borocd
LEFT JOIN tracts ON lift.boroct2020 = tracts.boroct2020
LEFT JOIN districts ON lift.cd = districts.borocd
{%- for col in category_columns %}
LEFT JOIN {{ col }}_spellings ON LOWER({{ squish.format('lift.' ~ col) }}) = {{ col }}_spellings.spelling_key
{%- endfor %}
