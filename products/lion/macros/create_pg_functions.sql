-- this file creates various postgres functions used in build

{% macro create_pg_functions() %}
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

--------------------------------------------------------------------------------------------
-- taken from https://www.spdba.com.au/cogo-finding-centre-and-radius-of-a-curve-defined-by-three-points-postgis/

/** ----------------------------------------------------------------------------------------
  * @function   : FindCircle
  * @precis     : Function that determines if three points form a circle. If so a table containing
  *               centre and radius is returned. If not, a null table is returned.
  * @version    : 1.0
  * @param      : p_pt1        : First point in curve
  * @param      : p_pt2        : Second point in curve
  * @param      : p_pt3        : Third point in curve
  * @return     : geometry     : In which X,Y ordinates are the centre X, Y and the Z being the radius of found circle
  *                              or NULL if three points do not form a circle.
  * @history    : Simon Greener - Feb 2012 - Original coding.
  *             : Finn van Krieken - Apr 2025 - formatting
  * @copyright  : Simon Greener @ 2012
  *               Licensed under a Creative Commons Attribution-Share Alike 2.5 Australia License. (http://creativecommons.org/licenses/by-sa/2.5/au/)
**/
CREATE OR REPLACE FUNCTION find_circle(p_pt1 geometry, p_pt2 geometry, p_pt3 geometry)
    RETURNS geometry AS
$BODY$
DECLARE
    v_Centre geometry;
    v_radius NUMERIC;
    v_CX     NUMERIC;
    v_CY     NUMERIC;
    v_dA     NUMERIC;
    v_dB     NUMERIC;
    v_dC     NUMERIC;
    v_dD     NUMERIC;
    v_dE     NUMERIC;
    v_dF     NUMERIC;
    v_dG     NUMERIC;
BEGIN
    IF ( p_pt1 IS NULL OR p_pt2 IS NULL OR p_pt3 IS NULL ) THEN
        RAISE EXCEPTION 'All supplied points must be not null.';
        RETURN NULL;
    END IF;
    IF ( ST_GeometryType(p_pt1) <> 'ST_Point' OR
         ST_GeometryType(p_pt1) <> 'ST_Point' OR
         ST_GeometryType(p_pt1) <> 'ST_Point' ) THEN
        RAISE EXCEPTION 'All supplied geometries must be points.';
        RETURN NULL;
    END IF;
    v_dA := ST_X(p_pt2) - ST_X(p_pt1);
    v_dB := ST_Y(p_pt2) - ST_Y(p_pt1);
    v_dC := ST_X(p_pt3) - ST_X(p_pt1);
    v_dD := ST_Y(p_pt3) - ST_Y(p_pt1);
    v_dE := v_dA * (ST_X(p_pt1) + ST_X(p_pt2)) + v_dB * (ST_Y(p_pt1) + ST_Y(p_pt2));
    v_dF := v_dC * (ST_X(p_pt1) + ST_X(p_pt3)) + v_dD * (ST_Y(p_pt1) + ST_Y(p_pt3));
    v_dG := 2.0  * (v_dA * (ST_Y(p_pt3) - ST_Y(p_pt2)) - v_dB * (ST_X(p_pt3) - ST_X(p_pt2)));
    -- If v_dG is zero then the three points are collinear and no finite-radius
    -- circle through them exists.
    IF ( v_dG = 0 ) THEN
        RETURN NULL;
    ELSE
        v_CX := (v_dD * v_dE - v_dB * v_dF) / v_dG;
        v_CY := (v_dA * v_dF - v_dC * v_dE) / v_dG;
        v_Radius := SQRT(POWER(ST_X(p_pt1) - v_CX,2) + POWER(ST_Y(p_pt1) - v_CY,2) );
    END IF;
    RETURN ST_SetSRID(ST_MakePoint(v_CX, v_CY, v_radius),ST_Srid(p_pt1));
END;
$BODY$
    LANGUAGE plpgsql

{% endmacro %}
