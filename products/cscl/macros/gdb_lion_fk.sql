{# Full row key for the LION gdb comparison; mirrors compare_gdb.py (lion_outputs.csv):
   SegmentID|Join_ID|LLo_Hyphen|RLo_Hyphen, with Join_ID and house numbers trimmed. #}
{% macro gdb_lion_fk(segmentid, join_id, llo_hyphen, rlo_hyphen) -%}
concat_ws('|', {{ segmentid }}, rtrim({{ join_id }}), trim({{ llo_hyphen }}), trim({{ rlo_hyphen }}))
{%- endmacro %}

{# Comparable text projection of the gdb_lion columns: lowercased names, every value trimmed
   with blank -> NULL (prod pads fixed-width fields with spaces; build leaves them NULL). #}
{% macro gdb_lion_text_columns(prefix_quote=true) -%}
{%- set cols = adapter.get_columns_in_relation(ref('gdb_lion')) -%}
{%- for c in cols if c.name | lower not in ['geom', 'shape_length', 'shape'] -%}
{%- set n = c.name | lower -%}
nullif(trim("{{ c.name if prefix_quote else n }}"::text), '') AS {{ n }}{{ ',' if not loop.last }}
{% endfor -%}
{%- endmacro %}
