{% macro config_value(key) -%}
{#-
  A scalar subquery for one seeds/config.csv value, by key - see that seed's
  description in seeds.yml for what belongs there and why (values GR's export
  tools take as operator input, nothing in source data records them).

  Always text - cast at the call site (e.g. ::date) if the value needs to be
  something else. Returns NULL if the key is missing rather than raising - a
  caller that needs to fail loudly on a missing key (e.g. int__ldf_header.sql)
  should check for it explicitly rather than relying on this macro to catch
  it, since different callers care about different keys.
-#}
(SELECT value FROM {{ ref('config') }} WHERE key = '{{ key }}')
{%- endmacro %}
