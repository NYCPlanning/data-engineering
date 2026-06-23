{#
  Join_ID for the ESRI LION (BYTES of the Big Apple) — links a LION segment to its
  AltNames entries. Per ETL spec §2.7.3, the non-SAF form is a 15-char string:
  Borough(1) + FaceCode(4) + LGC1..LGC4 (2 bytes each, zero-filled, '00' if null) + two spaces.
  e.g. boro 2, facecode 7280, lgc1 01 -> '2728001000000  '.
  Note: SAF-replicant Join_IDs (segments generated from SAF data) use a different
  encoding and are not produced here.
#}
{% macro lion_join_id(
    boro='boroughcode', facecode='face_code',
    lgc1='lgc1', lgc2='lgc2', lgc3='lgc3', lgc4='lgc4'
) %}
{{ boro }} || {{ facecode }}
    || coalesce({{ lgc1 }}, '00') || coalesce({{ lgc2 }}, '00')
    || coalesce({{ lgc3 }}, '00') || coalesce({{ lgc4 }}, '00') || '  '
{% endmacro %}
