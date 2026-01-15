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
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION offset_points(
    line geometry,
    midpoint geometry,
    offset_length numeric DEFAULT 1.0
) RETURNS RECORD AS
$BODY$
DECLARE
    srid integer;
    segments geometry[];
    mid_segment geometry;
    ref_p1 geometry;
    ref_p2 geometry;
    dx numeric;
    dy numeric;
    unit_corr numeric;
BEGIN
    IF (ST_GeometryType(line) <> 'ST_LineString') THEN
        -- RAISE EXCEPTION 'Input geom must be line';
        RETURN NULL;
    END IF;

    srid := ST_SRID(line);
    
	SELECT array_agg(dump.geom)
	INTO segments
	FROM ST_DumpSegments(line) AS dump
    WHERE ST_Intersects(dump.geom, midpoint);

    IF array_length(segments, 1) = 0 OR segments IS NULL THEN
        -- precision issue - find nearest segment
        WITH dists AS (
            SELECT dump.geom, dump.geom <-> midpoint AS dist
            FROM ST_DumpSegments(line) AS dump
        )
        SELECT geom
        INTO mid_segment
        FROM dists
        ORDER BY dist
        LIMIT 1;
    ELSIF array_length(segments, 1) = 1 THEN
        mid_segment := segments[1];
    ELSIF array_length(segments, 1) = 2 THEN
        -- TODO - midpoint is at vertex, perp should bisect angle
        --      - for now, just take first segment
        mid_segment := segments[1];
    ELSE
        RAISE EXCEPTION 'Geom error - more than two line segments matched to midpoint';
        RETURN NULL;
    END IF;

    ref_p1 := ST_PointN(mid_segment, 1);
    ref_p2 := ST_PointN(mid_segment, 2);

    dx = ST_Y(ref_p2) - ST_Y(ref_p1);
    dy = ST_X(ref_p1) - ST_X(ref_p2);

    unit_corr = offset_length / sqrt(power(dx, 2) + power(dy, 2));
    
    RETURN ( 
        ST_SetSRID(ST_MakePoint(ST_X(midpoint) - dx * unit_corr, ST_Y(midpoint) - dy * unit_corr), srid),
        ST_SetSRID(ST_MakePoint(ST_X(midpoint) + dx * unit_corr, ST_Y(midpoint) + dy * unit_corr), srid)
    );
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION convert_level_code(level_code integer, feature_type text)
    RETURNS text AS
$$
BEGIN
    IF feature_type = 'shoreline' THEN
        RETURN '$';
    ELSIF feature_type = 'nonstreetfeatures' OR level_code = 99 THEN
        RETURN '*';
    ELSIF level_code BETWEEN 1 AND 26 THEN
        RETURN chr(64 + level_code);
    ELSE
        RETURN NULL;
    END IF;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION linearize(
    geom geometry, 
    curve_to_line_tolerance numeric DEFAULT 0.01
) RETURNS geometry AS
$$
DECLARE
    geometry_type text;
BEGIN
    geometry_type := ST_GeometryType(geom);
    IF geometry_type = 'ST_LineString' THEN
        RETURN geom;
    ELSIF geometry_type = 'ST_MultiLineString' THEN
        RETURN ST_LineMerge(geom);
    ELSIF geometry_type = 'ST_MultiCurve' THEN
        RETURN ST_LineMerge(ST_CurveToLine(geom, curve_to_line_tolerance, 1));
    ELSIF geometry_type = 'ST_MultiSurface' THEN
        RETURN ST_CurveToLine(geom, curve_to_line_tolerance, 1);
    END IF;
    RETURN geom;
END;
$$
LANGUAGE plpgsql;

{% endmacro %}
