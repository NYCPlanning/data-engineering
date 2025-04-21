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
