/*
This function is for formatting text as specified in CSCL Phase II ETL Docs section 1.4
inputs
    value: the text value to be formatted
    n: the number of characters of the output text
    fill: character to fill with (for our uses, zero or space)
    blank_if_none: if true, if `value` is none then returns n-length spaces regardless of `fill`
    left_: if true, left-justifies instead of right
outputs
    formatted text of length `n`, `value` lpad or rpadded with `fill` arg
*/

{% macro create_format_text_proc() %}

CREATE OR REPLACE FUNCTION format_lion_text(
    value varchar,
    n int,
    fill varchar DEFAULT '0',
    blank_if_none boolean DEFAULT FALSE,
    left_ boolean DEFAULT FALSE
) RETURNS varchar AS $$

BEGIN
    IF value IS NULL THEN
        value := '';
        IF blank_if_none THEN
            fill := ' '; -- otherwise, remain null and fail check? Or fill with supplied fill?
        END IF;
    END IF;

    IF left_ THEN
        return rpad(value, n, fill);
    ELSE
        return lpad(value, n, fill);
    END IF;

END $$
LANGUAGE plpgsql;

{% endmacro %}
