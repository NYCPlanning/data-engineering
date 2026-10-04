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
    curve_to_line_tolerance numeric DEFAULT 0.00025
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

--------------------------------------------------------------------------------------------
-- test_cases.geometry_diff_metrics: a robustness-focused geometry comparison for the
-- test_cases.examples TDD table (see products/cscl chat log, 2026-10-04), complementing
-- plain SHAPE_Length/SHAPE_Area diffing (qa__diffs_fgdb_* models).
--
-- Deliberately schema-qualified to test_cases rather than created bare: test_cases is a
-- stable, shared schema in db-cscl, never touched by a branch's own build/recipe-reload
-- (which clears the *branch* schema, not this one - the mechanism that wiped the first,
-- ad hoc version of this work, see that chat log). Creating it here, inside the same
-- on-run-start hook every branch's `dbt build` already runs, means the function
-- redefines itself fresh on every build regardless of branch - no separate manual step,
-- and it can never again go stale or missing the way a one-off psql session's objects
-- did.
--
-- Inputs may be in any SRID (stored examples use 4326) - both are transformed to 2263
-- internally so every output is in real feet/sqft, not degrees.
--
-- expected may be NULL (a pure build-side artifact with no prod counterpart, e.g. a
-- GEOS overlay-robustness sliver with nothing on the prod side to compare against) - the
-- three expected-comparison columns come back NULL in that case; max_ring_eccentricity
-- still computes from actual alone.
--
-- Metrics, and why there are three instead of one:
--   symdiff_area_pct       - % of actual's own area that disagrees with expected.
--                             Catches real coverage gain/loss (a clipped-off pier, a
--                             spurious extra chunk of land) - the obvious "do these
--                             shapes basically match" check.
--   hausdorff_ft           - max boundary deviation between actual and expected.
--   symdiff_boundary_len_ft - perimeter of the symmetric-difference region.
--                             Both catch jagged/thin boundary defects that barely move
--                             enclosed area but wildly inflate perimeter - confirmed
--                             empirically on nycb2010 block 30666000002 (chat log
--                             2026-10-04): symdiff_area_pct was only 0.15% (invisible)
--                             despite a 15x real SHAPE_Length inflation, while
--                             hausdorff_ft (337.57) and symdiff_boundary_len_ft (770.31)
--                             both caught it clearly. symdiff_area_pct alone is NOT
--                             sufficient - a thread-thin sliver barely changes enclosed
--                             area no matter how long or jagged it is.
--   max_ring_eccentricity  - length / sqrt(area) of the single worst ring (interior or
--                             exterior, across all parts) in actual. Needs no expected
--                             geometry at all - catches structurally degenerate output
--                             (needle-shaped holes, GEOS overlay artifacts) on its own
--                             terms. A real, compact polygon stays in the single digits
--                             to low tens; the PUMA 4105 needle sliver measured 2885.76
--                             (chat log 2026-10-03). Note this is ST_Length, not
--                             ST_Perimeter - ST_ExteriorRing/ST_InteriorRingN return a
--                             LINESTRING, and ST_Perimeter is 0 by definition on
--                             anything non-areal (this silently broke the first version
--                             of this function - caught immediately because the needle
--                             test case scored 0 instead of ~2886, exactly the kind of
--                             bug this TDD setup exists to catch).
--   perimeter_pct_diff     - 100 * (actual's ST_Perimeter - expected's) / expected's,
--                             raw and unfiltered - directly mirrors the SHAPE_Length
--                             comparison compare_gdb.py/qa_int__prod_fgdb_* already do at
--                             the scalar-field level (CSCL-DISTRICTS-03), now available
--                             inside this framework too. Needed as its own metric, not
--                             redundant with the others: symdiff_area_pct/
--                             symdiff_boundary_len_ft only sum fragments >=
--                             min_fragment_sqft (100 by default in production use via
--                             geometry_diff_qa), and every needle-sliver fragment found
--                             so far is under a few sqft - so in real use those two
--                             metrics are structurally blind to exactly this defect
--                             class. hausdorff_ft catches it sometimes but isn't
--                             reliable either: confirmed on PUMA 3701 (chat log
--                             2026-10-05), the same underlying defect (a single needle
--                             inflating SHAPE_Length 22%) swung hausdorff_ft from 2,330
--                             ft down to 0.05 ft under clip_to_shoreline_gridsize=0.005,
--                             purely from grid-alignment luck, not a real change in how
--                             wrong the perimeter was before that fix. perimeter_pct_diff
--                             measures the one thing none of the others do directly: is
--                             the actual boundary length itself way off, independent of
--                             fragment-area filtering or Hausdorff's own luck.
--
-- symdiff_area_pct/symdiff_boundary_len_ft only sum symmetric-difference fragments
-- >= min_fragment_sqft (default 100, matching clipped_geom()'s own min_area floor).
-- Caught on nyfb/FireBN 43 (chat log 2026-10-05): the raw, unfiltered symmetric
-- difference against prod decomposed into 5,049 fragments, median area 0.0014 sqft -
-- pervasive hairline noise along a long, independently-digitized coastline, not real
-- defects (same mechanism as CSCL-DISTRICTS-03 generally) - summed together these
-- pushed symdiff_boundary_len_ft past 300,000 ft, swamping the one fragment that
-- actually mattered (91,531 sqft, a real discrepancy). Filtering first means these
-- two metrics reflect substantial disagreement only.
--
-- hausdorff_ft runs ST_Simplify (tolerance 0.05 ft, far below anything real this
-- project cares about) on both inputs first - ST_HausdorffDistance's cost scales with
-- vertex count, and a battalion-scale coastline carries thousands of near-collinear
-- vertices simplification can collapse for free without touching genuine corners/
-- deviations.
--
-- A first attempt restricted the Hausdorff call to a buffered region around the real
-- (area-filtered) symdifference fragments instead - much faster, but wrong twice
-- over, caught by the same two test cases that validated everything else here (chat
-- log 2026-10-05): it reported 0 for the nycb2010 block's actual ~338 ft, because
-- that block's own real defect is itself a near-zero-area needle (high extent, tiny
-- area - exactly what min_fragment_sqft is designed to treat as noise, wrongly, when
-- it's actually the thing a Hausdorff check exists to catch) and so got excluded from
-- the search region entirely; and it reported ~9,387 ft for FireBN 47's real ~1,988
-- ft, because ST_Intersection-ing each geometry down to that region creates a brand
-- new boundary edge exactly at the clip line, and that synthetic edge - not any real
-- disagreement - is what Hausdorff then measured. Simplification doesn't introduce
-- new edges or depend on an area threshold, so it doesn't have either failure mode.
CREATE SCHEMA IF NOT EXISTS test_cases;

-- CREATE OR REPLACE only replaces a function with the exact same parameter list - it
-- adds a new overload instead of replacing when the signature changes (as it did here,
-- gaining min_fragment_sqft), leaving the old 2-arg version callable alongside the new
-- one and making every 2-arg call ambiguous ("is not unique") until the stale overload
-- is dropped explicitly (broke the very next build after this param was added, chat
-- log 2026-10-05). Same gotcha applies to adding perimeter_pct_diff below - a new
-- output column is a return-type change too, so the previous 3-arg signature must be
-- dropped explicitly again, not just the old 2-arg one.
DROP FUNCTION IF EXISTS test_cases.geometry_diff_metrics(geometry, geometry);
DROP FUNCTION IF EXISTS test_cases.geometry_diff_metrics(geometry, geometry, double precision);

CREATE OR REPLACE FUNCTION test_cases.geometry_diff_metrics(
    actual geometry, expected geometry, min_fragment_sqft double precision DEFAULT 100
)
RETURNS TABLE (
    actual_area_sqft double precision,
    expected_area_sqft double precision,
    symdiff_area_pct double precision,
    hausdorff_ft double precision,
    symdiff_boundary_len_ft double precision,
    symdiff_fragment_count integer,
    max_ring_eccentricity double precision,
    perimeter_pct_diff double precision
) AS $$
    -- linearize() (defined above) first: prod's raw FGDB load can carry curved
    -- MultiSurface/MultiCurve geometry, which ST_Simplify (used below for
    -- hausdorff_ft) rejects outright - straighten it the same way every stg__ model
    -- already does for its own source geometry, rather than requiring every caller
    -- of this function to remember to.
    --
    -- Turned out to fix more than a crash: nyfb (chat log 2026-10-05) had several
    -- rows (e.g. FireBN 3/4) reporting a real-looking symdiff_boundary_len_ft purely
    -- because prod's still-curved shape and our always-straight-line build output are
    -- two different representations of arguably the same boundary - comparing a curve
    -- against its chord approximation looks like disagreement even with zero real
    -- difference. Linearizing both sides the same way before comparing collapsed
    -- those to a clean 0 while leaving genuine discrepancies on other MultiSurface
    -- rows (e.g. FireBN 20) exactly where they were - confirmed directly via
    -- ST_GeometryType on production_outputs.fgdb_nyfb.shape, not just inferred.
    WITH a AS (SELECT st_transform(linearize(actual), 2263) AS geom),
    e AS (SELECT st_transform(linearize(expected), 2263) AS geom),
    parts AS (SELECT (st_dump(a.geom)).geom AS part_geom FROM a),
    rings AS (
        SELECT st_exteriorring(part_geom) AS ring FROM parts
        UNION ALL
        SELECT st_interiorringn(part_geom, gs) AS ring
        FROM parts, generate_series(1, st_numinteriorrings(part_geom)) AS gs
    ),
    ring_eccentricity AS (
        SELECT max(
            st_length(ring) / greatest(sqrt(abs(st_area(st_makepolygon(ring)))), 0.0001)
        ) AS value
        FROM rings
    ),
    real_fragments AS (
        SELECT (st_dump(st_symdifference(a.geom, e.geom))).geom AS frag
        FROM a, e
        WHERE e.geom IS NOT NULL
    ),
    real_fragments_filtered AS (
        SELECT frag FROM real_fragments WHERE st_area(frag) >= min_fragment_sqft
    )
    SELECT
        st_area(a.geom),
        st_area(e.geom),
        CASE WHEN e.geom IS NOT NULL THEN
            (SELECT coalesce(sum(st_area(frag)), 0) FROM real_fragments_filtered)
            / nullif(st_area(a.geom), 0) * 100
        END,
        CASE WHEN e.geom IS NOT NULL THEN
            st_hausdorffdistance(st_simplify(a.geom, 0.05), st_simplify(e.geom, 0.05))
        END,
        CASE WHEN e.geom IS NOT NULL THEN
            (SELECT coalesce(sum(st_perimeter(frag)), 0) FROM real_fragments_filtered)
        END,
        CASE WHEN e.geom IS NOT NULL THEN (SELECT count(*)::int FROM real_fragments_filtered) END,
        (SELECT value FROM ring_eccentricity),
        CASE WHEN e.geom IS NOT NULL THEN
            (st_perimeter(a.geom) - st_perimeter(e.geom)) / nullif(st_perimeter(e.geom), 0) * 100
        END
    FROM a, e
$$ LANGUAGE sql IMMUTABLE;

{% endmacro %}
