{#
    Split a whole number across geographies so the parts still sum to it.

    Rounding each part on its own loses or gains units, so the same projects
    add up differently depending on which geography you aggregate by. This
    gives each geography its floor, then hands the leftover units to whichever
    geographies were closest to rounding up.

    A project in a single geography has a share of 1, so its floor is the whole
    number and there is nothing left over. Only split projects are affected.

    NULL measures stay NULL: floor(NULL) is NULL, and the leftover comparison
    is NULL, which the CASE reads as no extra unit.
#}

{% macro largest_remainder(measure, share, partition_by, tiebreak) -%}
floor({{ measure }} * {{ share }})
+ CASE
    WHEN
        row_number() OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY
                ({{ measure }} * {{ share }})
                - floor({{ measure }} * {{ share }}) DESC,
                {{ tiebreak }} ASC
        )
        <= {{ measure }}
        - sum(floor({{ measure }} * {{ share }})) OVER (
            PARTITION BY {{ partition_by }}
        )
        THEN 1
    ELSE 0
END
{%- endmacro %}
