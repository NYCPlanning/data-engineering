-- Every known-difference ID a qa__diffs_* model cites in a label exists in the
-- known_differences seed and isn't fixed. A cite is a string literal starting with an ID
-- ('CSCL-LION-16: topology left/right AP') or an old bug number ('Bug 003: ...'). An old
-- bug number, or anything that isn't an exact ID ('CSCL-LION-0_:%'), fails: cite the
-- seed's id instead. Reads the models' SQL, not their output, so it needs only the seed.
{%- set cites = [] %}
{%- if execute %}
    {%- for node in graph.nodes.values() %}
        {%- if node.resource_type == 'model' and node.name.startswith('qa__diffs_') %}
            {%- for cited in modules.re.findall("'((?:CSCL-|Bug )[^':]*)", node.raw_code) %}
                {%- do cites.append({'model': node.name, 'cited': cited}) %}
            {%- endfor %}
        {%- endif %}
    {%- endfor %}
{%- endif %}

WITH cited AS (
    {%- for c in cites %}
        SELECT
            '{{ c.model }}' AS model,
            '{{ c.cited }}' AS cited
        {% if not loop.last %}UNION{% endif %}
    {%- else %}
        SELECT
            NULL::varchar AS model,
            NULL::varchar AS cited
        WHERE FALSE
    {%- endfor %}
),

checked AS (
    SELECT
        cited.model,
        cited.cited,
        CASE
            WHEN NOT regexp_full_match(cited.cited, 'CSCL-[A-Z]+-[0-9]{2}')
                THEN 'not an exact known-difference ID'
            WHEN kd.id IS NULL THEN 'not in known_differences'
            WHEN kd.status = 'fixed' THEN 'known difference is fixed'
        END AS problem
    FROM cited
    LEFT JOIN {{ ref('known_differences') }} AS kd ON cited.cited = kd.id
)

SELECT * FROM checked
WHERE problem IS NOT NULL
