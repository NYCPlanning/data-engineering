-- The log__* models and the log_messages seed agree. Every log_messages id a log__* model
-- cites ('CSCL-LOG-LION-05') exists in the seed; every seed row with status implemented
-- names, in its model column, a log__* model whose SQL cites that id; and no other row
-- names a model. Reads the models' SQL, not their output, so it needs only the seed (the
-- relationships test on log.log_message_id checks the built rows).
{%- set cites = [] %}
{%- set log_models = {} %}
{%- if execute %}
    {%- for node in graph.nodes.values() %}
        {%- if node.resource_type == 'model' and node.name.startswith('log__') %}
            {%- set ids = modules.re.findall("CSCL-LOG-[A-Z]+-[0-9]+", node.raw_code) | unique | list %}
            {%- do log_models.update({node.name: ids}) %}
            {%- for cited in ids %}
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
        {% if not loop.last %}UNION ALL{% endif %}
    {%- else %}
        SELECT
            NULL::varchar AS model,
            NULL::varchar AS cited
        WHERE FALSE
    {%- endfor %}
),

log_models AS (
    {%- for name in log_models %}
        SELECT '{{ name }}' AS model
        {% if not loop.last %}UNION ALL{% endif %}
    {%- endfor %}
),

problems AS (
    SELECT
        cited.model,
        cited.cited AS log_message_id,
        'cited id not in log_messages' AS problem
    FROM cited
    LEFT JOIN {{ ref('log_messages') }} AS lm ON cited.cited = lm.id
    WHERE lm.id IS NULL
    UNION ALL
    SELECT
        lm.model,
        lm.id AS log_message_id,
        CASE
            WHEN lm.model IS NULL THEN 'implemented but no model named'
            WHEN log_models.model IS NULL THEN 'model is not a log__* model'
            ELSE 'model does not cite this id'
        END AS problem
    FROM {{ ref('log_messages') }} AS lm
    LEFT JOIN log_models ON lm.model = log_models.model
    LEFT JOIN cited ON lm.model = cited.model AND lm.id = cited.cited
    WHERE lm.status = 'implemented' AND cited.cited IS NULL
    UNION ALL
    SELECT
        lm.model,
        lm.id AS log_message_id,
        'model named on a row that is not implemented' AS problem
    FROM {{ ref('log_messages') }} AS lm
    WHERE lm.status <> 'implemented' AND lm.model IS NOT NULL
)

SELECT * FROM problems
