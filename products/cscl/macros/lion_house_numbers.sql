{#
  gdb_lion house-number helpers.

  lion_house_number_split: the four house-number values (l_low, l_high, r_low, r_high) for
  output feature `n` (1 or 2) of a segment/SAF entry, as a LATERAL-able subquery.
  - parity = 'L' or 'R' (continuous parity, ETL spec §2.7.1): the segment is modeled as two
    coincident features, feature 1 carrying the first (nominally left) range and feature 2 the
    second (nominally right) range, each stored in the house-number fields of the side named
    by `parity`, with the other side empty.
  - otherwise both sides pass through as given (n is always 1).
  - not addressable: everything is empty. Legacy rule (BytesLION_CSCL_workflow_tool_PUB.py):
    SegmentTyp IN ('G', 'F') OR FeatureTyp NOT IN ('0', '6', 'W'). The ETL spec's version of
    this rule differs; we follow the legacy tool.

  lion_normalized_house_number: FromLeft/ToLeft/FromRight/ToRight form (ETL spec §2.7.3): the
  hyphen is dropped and the digits after it are zero-filled to three; non-hyphenated numbers
  are unchanged. Empty -> 0.
#}
{% macro lion_house_number_split(l_low, l_high, r_low, r_high, parity, n, addressable) %}
SELECT
    CASE
        WHEN NOT {{ addressable }} THEN NULL
        WHEN {{ parity }} = 'L' THEN CASE WHEN {{ n }} = 1 THEN {{ l_low }} ELSE {{ r_low }} END
        WHEN {{ parity }} = 'R' THEN NULL
        ELSE {{ l_low }}
    END AS l_low,
    CASE
        WHEN NOT {{ addressable }} THEN NULL
        WHEN {{ parity }} = 'L' THEN CASE WHEN {{ n }} = 1 THEN {{ l_high }} ELSE {{ r_high }} END
        WHEN {{ parity }} = 'R' THEN NULL
        ELSE {{ l_high }}
    END AS l_high,
    CASE
        WHEN NOT {{ addressable }} THEN NULL
        WHEN {{ parity }} = 'R' THEN CASE WHEN {{ n }} = 1 THEN {{ l_low }} ELSE {{ r_low }} END
        WHEN {{ parity }} = 'L' THEN NULL
        ELSE {{ r_low }}
    END AS r_low,
    CASE
        WHEN NOT {{ addressable }} THEN NULL
        WHEN {{ parity }} = 'R' THEN CASE WHEN {{ n }} = 1 THEN {{ l_high }} ELSE {{ r_high }} END
        WHEN {{ parity }} = 'L' THEN NULL
        ELSE {{ r_high }}
    END AS r_high
{% endmacro %}

{% macro lion_normalized_house_number(hn) %}
coalesce(
    CASE
        WHEN {{ hn }} LIKE '%-%'
            THEN (
                regexp_replace(split_part({{ hn }}, '-', 1), '\D', '', 'g')
                || lpad(regexp_replace(split_part({{ hn }}, '-', 2), '\D', '', 'g'), 3, '0')
            )::int
        ELSE nullif(regexp_replace({{ hn }}, '\D', '', 'g'), '')::int
    END, 0
)
{% endmacro %}
