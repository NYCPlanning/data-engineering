"""Break a few loaded input records on purpose, so we can check log.csv catches each one.

Each mutation in MUTATIONS breaks records in the loaded source tables in a way that should
make one or more log_messages ids fire (seeds/log_messages.csv). The script picks
otherwise-clean records (not already in log.csv, when a baseline run exists), keeps every
injection on its own records, applies the edits, and writes a manifest table
(chaos_injections, one row per injection and expected log id). qa__chaos_injections and
qa__chaos_summary then check recall (every injected record logged under each expected id),
precision (no new log rows beyond the injected records) and effect (records the message
says are dropped really are gone).

Run on a throwaway copy of an inputs-loaded DuckDB (DUCKDB_PATH, BUILD_ENGINE_SCHEMA):

    dbt seed && dbt run                    # optional baseline: lets precision ignore old rows
    python poc_validation/chaos_monkey.py --seed 1
    dbt run
    python poc_validation/chaos_monkey.py --report

Mutations for planned ids are expected to fail recall until their log model lands.
"""

import argparse
import os
import random
from dataclasses import dataclass, field

import duckdb

MANIFEST = "chaos_injections"
BASELINE_LOG = "chaos_baseline_log"
# segment ids no source table uses
FAKE_SEGMENTID_BASE = 990_000_000


@dataclass
class Injection:
    mutation: str
    log_message_ids: list[str]
    table_name: str
    key_column: str
    key_value: str
    column_name: str
    old_value: str | None
    new_value: str | None
    # how the expected log row is found: the value in log's globalid or record_id, or a
    # string in its message (ThinLION lookups carry no key)
    log_match_column: str
    log_match_value: str
    # 'lion', 'saf', 'snd', 'enders' or 'rpl': the output the record must be missing from
    # (dropped messages)
    dropped_from: str | None = None
    dropped_globalid: str | None = None
    # ids the edit can fire on the same record, depending on its surroundings: not
    # required, but their rows don't count against precision
    may_log_ids: list[str] = field(default_factory=list)
    # per-id message match, where an id's row carries a different key
    message_matches: dict[str, str] = field(default_factory=dict)


@dataclass
class Mutation:
    """Set `column` to a new value on records picked from `candidates_sql`.

    candidates_sql selects one row per candidate record, ordered for reproducibility, with
    columns: key (value of key_column in `table`), claims (list of ids the injection uses
    up: segment/record globalids and ids, so injections never share one), log_key (value
    expected in the log row), old (the value being replaced). {s} is the schema.
    """

    name: str
    log_message_ids: list[str]
    table: str
    column: str
    candidates_sql: str
    new_value: object = None
    key_column: str = "globalid"
    log_match_column: str = "globalid"
    dropped_from: str | None = None
    may_log_ids: list[str] = field(default_factory=list)
    # log id -> f(candidate) giving a string its message contains
    message_matches: dict = field(default_factory=dict)
    extra: dict = field(default_factory=dict)

    def value_for(self, cand: dict, n: int) -> object:
        return self.new_value(cand, n) if callable(self.new_value) else self.new_value

    def apply(
        self, conn: duckdb.DuckDBPyConnection, schema: str, cand: dict, n: int
    ) -> Injection:
        new = self.value_for(cand, n)
        conn.execute(
            f'UPDATE "{schema}"."{self.table}" SET "{self.column}" = ? WHERE "{self.key_column}" = ?',
            [new, cand["key"]],
        )
        return self._injection(cand, None if new is None else str(new))

    def _injection(
        self, cand: dict, new: str | None, column: str | None = None
    ) -> Injection:
        return Injection(
            mutation=self.name,
            log_message_ids=self.log_message_ids,
            table_name=self.table,
            key_column=self.key_column,
            key_value=str(cand["key"]),
            column_name=column or self.column,
            old_value=None if cand["old"] is None else str(cand["old"]),
            new_value=new,
            log_match_column=self.log_match_column,
            log_match_value=str(cand["log_key"]),
            dropped_from=self.dropped_from,
            dropped_globalid=cand.get("dropped_globalid"),
            may_log_ids=self.may_log_ids,
            message_matches={i: f(cand) for i, f in self.message_matches.items()},
        )


class SwapStreetCode(Mutation):
    """Replace a segment_lgc row's street code (b5sc and b7sc) with the candidate's new_b5sc/new_b7sc."""

    def apply(self, conn, schema, cand, n):
        conn.execute(
            f'UPDATE "{schema}".dcp_cscl_segment_lgc SET b5sc = ?, b7sc = ? WHERE globalid = ?',
            [cand["new_b5sc"], cand["new_b7sc"], cand["key"]],
        )
        return self._injection(cand, cand["new_b7sc"])


class MessageKey(Mutation):
    """A mutation whose log row is found by a distinctive new value in its message."""

    def apply(self, conn, schema, cand, n):
        inj = super().apply(conn, schema, cand, n)
        inj.log_match_value = inj.new_value or ""
        return inj


class RetractAtomicPolygons(MessageKey):
    """Move every AtomicPolygon of one tract (candidate borough, old) to a tract that doesn't exist."""

    def apply(self, conn, schema, cand, n):
        new = self.value_for(cand, n)
        conn.execute(
            f"""UPDATE "{schema}"."{self.table}" SET "{self.column}" = ?
            WHERE borough = ? AND "{self.column}" = ?""",
            [new, cand["borough"], cand["old"]],
        )
        inj = self._injection(cand, str(new))
        inj.log_match_value = str(new)
        return inj


class AddLgcs(Mutation):
    """Add ten extra non-preferred LGC rows to a segment (more than 9 LGCs)."""

    def apply(self, conn, schema, cand, n):
        conn.execute(
            f"""
            INSERT INTO "{schema}".dcp_cscl_segment_lgc
            SELECT lgc.* REPLACE (
                'N' AS preferred_lgc_flag,
                'N' AS boe_preferred_lgc_flag,
                new.lgc AS lgc,
                lgc.b5sc || new.lgc AS b7sc,
                'chaos-' || lgc.globalid || '-' || new.lgc AS globalid
            )
            FROM "{schema}".dcp_cscl_segment_lgc AS lgc,
                (SELECT 'Z' || i::VARCHAR AS lgc FROM range(10) t(i)) AS new
            WHERE lgc.globalid = ?
            """,
            [cand["lgc_globalid"]],
        )
        return self._injection(cand, str(int(cand["old"]) + 10))


class MoveOffshore(Mutation):
    """Move a segment's geometry out of the city, into water no AtomicPolygon covers.

    Candidates carry the start point (sx, sy); each injection lands 2,000 ft from the last.
    """

    def apply(self, conn, schema, cand, n):
        x, y = 800000 - n * 2000, 100000
        conn.execute(
            f'UPDATE "{schema}"."{self.table}" SET geom = ST_Translate(geom, ?, ?) WHERE globalid = ?',
            [x - cand["sx"], y - cand["sy"], cand["key"]],
        )
        return self._injection(cand, f"starts at {x} {y}", "geom")


class FlattenCurve(Mutation):
    """Mark a straight segment as a curve, bent by a hair (or not at all) at its middle.

    extra['sagitta'] is the bend in feet; a hair-thin bend puts the circle's center
    millions of feet away, a zero bend leaves no circle at all. Candidates carry the
    start and end points (sx, sy, ex, ey).
    """

    def apply(self, conn, schema, cand, n):
        sx, sy, ex, ey = cand["sx"], cand["sy"], cand["ex"], cand["ey"]
        length = ((ex - sx) ** 2 + (ey - sy) ** 2) ** 0.5
        bend = self.extra["sagitta"]
        mx = (sx + ex) / 2 - (ey - sy) / length * bend
        my = (sy + ey) / 2 + (ex - sx) / length * bend
        conn.execute(
            f"""UPDATE "{schema}"."{self.table}"
            SET curve = 'L', geom = ST_GeomFromText(?) WHERE globalid = ?""",
            [f"MULTILINESTRING (({sx} {sy}, {mx} {my}, {ex} {ey}))", cand["key"]],
        )
        return self._injection(cand, f"L, middle bent {bend} ft", "curve")


class DeleteNode(Mutation):
    """Delete the node at a dead end, so the one segment ending there has no node."""

    def apply(self, conn, schema, cand, n):
        conn.execute(
            f'DELETE FROM "{schema}".dcp_cscl_nodes WHERE globalid = ?', [cand["key"]]
        )
        return self._injection(cand, None, "(row deleted)")


def _fake_segmentid(_cand: dict, n: int) -> int:
    return FAKE_SEGMENTID_BASE + n


# Straight two-point centerlines with no proto or SAF row on their segmentid and no arcs,
# with their start and end points (sx, sy, ex, ey).
_LONE_CENTERLINE = """
    lone AS (
        SELECT
            cl.*,
            ST_X(ST_StartPoint(ST_LineMerge(cl.geom))) AS sx,
            ST_Y(ST_StartPoint(ST_LineMerge(cl.geom))) AS sy,
            ST_X(ST_EndPoint(ST_LineMerge(cl.geom))) AS ex,
            ST_Y(ST_EndPoint(ST_LineMerge(cl.geom))) AS ey
        FROM {s}.dcp_cscl_centerline AS cl
        WHERE
            cl.segmentid NOT IN (SELECT segmentid FROM {s}.dcp_cscl_altsegmentdata WHERE segmentid IS NOT NULL)
            AND ST_GeometryType(ST_LineMerge(cl.geom)) = 'LINESTRING'
            AND ST_NPoints(cl.geom) = 2
            AND cl.curve = 'S'
            AND cl.ogc_fid NOT IN (SELECT ogc_fid FROM {s}.dcp_cscl_centerline_arcs)
    )
"""

# How many line ends (centerline, shoreline, rail, subway, non-street feature) meet at each
# point, rounded to the hundredth of a foot.
_LINE_ENDS = """
    all_lines AS (
        SELECT geom FROM {s}.dcp_cscl_centerline
        UNION ALL SELECT geom FROM {s}.dcp_cscl_shoreline
        UNION ALL SELECT geom FROM {s}.dcp_cscl_rail
        UNION ALL SELECT geom FROM {s}.dcp_cscl_subway
        UNION ALL SELECT geom FROM {s}.dcp_cscl_nonstreetfeatures
    ),
    line_ends AS (
        SELECT x, y, COUNT(*) AS n
        FROM (
            SELECT ROUND(ST_X(ST_StartPoint(ST_LineMerge(geom))), 2) AS x,
                ROUND(ST_Y(ST_StartPoint(ST_LineMerge(geom))), 2) AS y
            FROM all_lines
            UNION ALL
            SELECT ROUND(ST_X(ST_EndPoint(ST_LineMerge(geom))), 2),
                ROUND(ST_Y(ST_EndPoint(ST_LineMerge(geom))), 2)
            FROM all_lines
        )
        GROUP BY ALL
    )
"""

# Centerlines that can leave LION (dropped, or moved away from their nodes) without changing
# a neighbour's output: neither end meets exactly one other line. There, the neighbour
# would become a dead end and its SEGLOCSTATUS rule change (H to I), logging
# CSCL-LOG-LION-01 on a record nobody injected.
_DETACHABLE_CENTERLINE = (
    _LINE_ENDS
    + """,
    detachable AS (
        SELECT cl.globalid
        FROM {s}.dcp_cscl_centerline AS cl
        INNER JOIN line_ends AS s
            ON s.x = ROUND(ST_X(ST_StartPoint(ST_LineMerge(cl.geom))), 2)
            AND s.y = ROUND(ST_Y(ST_StartPoint(ST_LineMerge(cl.geom))), 2)
        INNER JOIN line_ends AS e
            ON e.x = ROUND(ST_X(ST_EndPoint(ST_LineMerge(cl.geom))), 2)
            AND e.y = ROUND(ST_Y(ST_EndPoint(ST_LineMerge(cl.geom))), 2)
        WHERE s.n <> 2 AND e.n <> 2
    )
"""
)

MUTATIONS: list[Mutation] = [
    Mutation(
        name="centerline_seglocstatus",
        log_message_ids=["CSCL-LOG-LION-01"],
        table="dcp_cscl_centerline",
        column="seglocstatus",
        new_value="Z",
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, seglocstatus AS old
            FROM {s}.dcp_cscl_centerline ORDER BY globalid
        """,
    ),
    Mutation(
        name="proto_seglocstatus",
        log_message_ids=["CSCL-LOG-LION-02"],
        table="dcp_cscl_altsegmentdata",
        column="seglocstatus",
        new_value="Z",
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, seglocstatus AS old
            FROM {s}.dcp_cscl_altsegmentdata
            WHERE alt_segdata_type IS DISTINCT FROM 'S' AND segmentid IS NOT NULL
            ORDER BY globalid
        """,
    ),
    Mutation(
        name="centerline_other_borough",
        log_message_ids=["CSCL-LOG-LION-03"],
        table="dcp_cscl_centerline",
        column="boroughcode",
        new_value=lambda cand, _n: str(int(cand["old"]) % 5 + 1),
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, boroughcode AS old
            FROM {s}.dcp_cscl_centerline
            WHERE boroughcode IN ('1', '2', '3', '4', '5')
                AND segmentid NOT IN (SELECT segmentid FROM {s}.dcp_cscl_altsegmentdata WHERE segmentid IS NOT NULL)
            ORDER BY globalid
        """,
    ),
    SwapStreetCode(
        name="shoreline_other_borough_street_code",
        log_message_ids=["CSCL-LOG-LION-04"],
        table="dcp_cscl_segment_lgc",
        column="b7sc",
        # a shoreline's borough comes from its preferred street code: swap in another
        # borough's shoreline street code
        candidates_sql="""
            WITH pref AS (
                SELECT lgc.globalid, lgc.segmentid, lgc.b5sc, lgc.lgc, lgc.b7sc, sh.globalid AS sh_globalid
                FROM {s}.dcp_cscl_segment_lgc AS lgc
                INNER JOIN {s}.dcp_cscl_shoreline AS sh ON lgc.segmentid = sh.segmentid
                WHERE lgc.preferred_lgc_flag = 'Y'
            ),
            one_lgc AS (
                SELECT segmentid FROM {s}.dcp_cscl_segment_lgc GROUP BY segmentid HAVING COUNT(*) = 1
            ),
            donor AS (
                SELECT LEFT(b5sc, 1) AS boro, MIN(b5sc) AS b5sc, MIN(b7sc) AS b7sc
                FROM pref WHERE LEFT(b7sc, 6) = b5sc GROUP BY 1
            )
            SELECT pref.globalid AS key, [pref.sh_globalid, pref.segmentid::VARCHAR] AS claims,
                pref.sh_globalid AS log_key, pref.b7sc AS old,
                donor.b5sc AS new_b5sc, donor.b7sc AS new_b7sc
            FROM pref
            INNER JOIN one_lgc USING (segmentid)
            INNER JOIN donor ON donor.boro = (LEFT(pref.b5sc, 1)::INT % 5 + 1)::VARCHAR
            ORDER BY pref.globalid
        """,
    ),
    MoveOffshore(
        name="segment_offshore",
        # off in the water: no AtomicPolygon either side, no NYPD beat, no node at either end
        log_message_ids=["CSCL-LOG-LION-05", "CSCL-LOG-OURS-01", "CSCL-LOG-OURS-03"],
        table="dcp_cscl_centerline",
        column="geom",
        candidates_sql="WITH "
        + _LONE_CENTERLINE
        + ","
        + _DETACHABLE_CENTERLINE
        + """
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, sx || ' ' || sy AS old, sx, sy
            FROM lone WHERE ST_Length(geom) < 200
                AND globalid IN (SELECT globalid FROM detachable)
            ORDER BY globalid
        """,
    ),
    FlattenCurve(
        name="centerline_hairline_curve",
        log_message_ids=["CSCL-LOG-LION-07"],
        table="dcp_cscl_centerline",
        column="curve",
        extra={"sagitta": 0.0001},
        candidates_sql="WITH "
        + _LONE_CENTERLINE
        + """
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, curve AS old, sx, sy, ex, ey
            FROM lone WHERE ST_Length(geom) > 300 ORDER BY globalid
        """,
    ),
    AddLgcs(
        name="segment_ten_more_lgcs",
        log_message_ids=["CSCL-LOG-LION-17"],
        table="dcp_cscl_segment_lgc",
        column="(row count for segment)",
        candidates_sql="""
            WITH counts AS (
                SELECT segmentid, COUNT(*) AS n FROM {s}.dcp_cscl_segment_lgc GROUP BY segmentid
            )
            SELECT cl.segmentid::VARCHAR AS key, [cl.globalid, cl.segmentid::VARCHAR] AS claims,
                cl.globalid AS log_key, counts.n AS old, lgc.globalid AS lgc_globalid
            FROM {s}.dcp_cscl_centerline AS cl
            INNER JOIN counts USING (segmentid)
            INNER JOIN {s}.dcp_cscl_segment_lgc AS lgc
                ON cl.segmentid = lgc.segmentid AND lgc.preferred_lgc_flag = 'Y'
            ORDER BY cl.globalid
        """,
        key_column="segmentid",
    ),
    Mutation(
        name="streetname_blank_facecode",
        # LION-18 for the one centerline named from this B7SC; FC-01/02 are the face file's
        log_message_ids=["CSCL-LOG-LION-18", "CSCL-LOG-FC-01", "CSCL-LOG-FC-02"],
        # the face file's rows carry the StreetName row, not the centerline
        message_matches={
            "CSCL-LOG-FC-01": lambda cand: f"STREETNAMEID = [{cand['streetnameid']}]",
            "CSCL-LOG-FC-02": lambda cand: f"STREETNAMEID = [{cand['streetnameid']}]",
        },
        table="dcp_cscl_streetname",
        column="facecode",
        new_value=None,
        candidates_sql="WITH "
        + _DETACHABLE_CENTERLINE
        + """,
            principal AS (
                SELECT b7sc FROM {s}.dcp_cscl_streetname WHERE principal_flag = 'Y'
                UNION ALL
                SELECT b7sc FROM {s}.dcp_cscl_featurename WHERE principal_flag = 'Y'
            ),
            single_principal AS (SELECT b7sc FROM principal GROUP BY b7sc HAVING COUNT(*) = 1),
            pref AS (
                SELECT b7sc, MIN(segmentid) AS segmentid, COUNT(*) AS n
                FROM {s}.dcp_cscl_segment_lgc WHERE preferred_lgc_flag = 'Y' GROUP BY b7sc
            )
            SELECT sn.globalid AS key,
                [sn.globalid, sn.b7sc, cl.globalid, cl.segmentid::VARCHAR] AS claims,
                cl.globalid AS log_key, sn.facecode AS old, cl.globalid AS dropped_globalid,
                sn.streetnameid
            FROM {s}.dcp_cscl_streetname AS sn
            INNER JOIN single_principal USING (b7sc)
            INNER JOIN pref USING (b7sc)
            INNER JOIN {s}.dcp_cscl_centerline AS cl ON pref.segmentid = cl.segmentid
            WHERE sn.principal_flag = 'Y' AND sn.facecode IS NOT NULL AND pref.n = 1
                AND sn.b7sc NOT IN (
                    SELECT b5sc || lgc1 FROM {s}.dcp_cscl_altsegmentdata WHERE b5sc IS NOT NULL
                )
                AND cl.segmentid NOT IN (SELECT segmentid FROM {s}.dcp_cscl_altsegmentdata WHERE segmentid IS NOT NULL)
                AND cl.globalid IN (SELECT globalid FROM detachable)
            ORDER BY sn.globalid
        """,
        dropped_from="lion",
    ),
    Mutation(
        name="proto_unnamed_street_code",
        log_message_ids=["CSCL-LOG-LION-19"],
        table="dcp_cscl_altsegmentdata",
        column="b5sc",
        # borough digit + 99 + a counter: no such street code exists
        new_value=lambda cand, n: f"{cand['old'][0]}99{n % 1000:03d}",
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, b5sc AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_altsegmentdata
            WHERE alt_segdata_type IS DISTINCT FROM 'S' AND segmentid IS NOT NULL
                AND LENGTH(b5sc) = 6
            ORDER BY globalid
        """,
        dropped_from="lion",
    ),
    Mutation(
        name="nonstreetfeature_linetype_7",
        log_message_ids=["CSCL-LOG-LION-23"],
        table="dcp_cscl_nonstreetfeatures",
        column="linetype",
        new_value=7,
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, linetype AS old
            FROM {s}.dcp_cscl_nonstreetfeatures ORDER BY globalid
        """,
    ),
    Mutation(
        name="commonplace_blank_b7sc",
        log_message_ids=["CSCL-LOG-SAF-22"],
        table="dcp_cscl_commonplace_gdb",
        column="b7sc",
        new_value="",
        candidates_sql="""
            SELECT globalid AS key, [globalid, placeid::VARCHAR] AS claims,
                globalid AS log_key, b7sc AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_commonplace_gdb
            WHERE saftype IN ('G', 'N', 'X') AND security_level IS DISTINCT FROM 3
            ORDER BY globalid
        """,
        dropped_from="saf",
    ),
    Mutation(
        name="commonplace_unnamed_b7sc",
        log_message_ids=["CSCL-LOG-SAF-23"],
        table="dcp_cscl_commonplace_gdb",
        column="b7sc",
        new_value=lambda cand, n: f"{cand['old'][0]}99{n % 1000:03d}01",
        candidates_sql="""
            SELECT globalid AS key, [globalid, placeid::VARCHAR] AS claims,
                globalid AS log_key, b7sc AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_commonplace_gdb
            WHERE saftype IN ('G', 'N', 'X') AND security_level IS DISTINCT FROM 3
                AND LENGTH(b7sc) = 8
            ORDER BY globalid
        """,
        dropped_from="saf",
    ),
    MessageKey(
        name="atomicpolygon_missing_tract_2020",
        log_message_ids=["CSCL-LOG-THIN-01"],
        table="dcp_cscl_atomicpolygons",
        column="censustract_2020",
        new_value=lambda _cand, n: f"99{n % 10000:04d}",
        candidates_sql="""
            SELECT globalid AS key, [globalid] AS claims, globalid AS log_key,
                censustract_2020 AS old
            FROM {s}.dcp_cscl_atomicpolygons ORDER BY globalid
        """,
        log_match_column="message",
    ),
    RetractAtomicPolygons(
        name="atomicpolygons_missing_tract_2010",
        # every AP of one tract, so no segment gains a tract line between its two sides (the
        # LION rules give 'X' there, CSCL-LOG-LION-01)
        # its APs lose their health area too (looked up through the 2010 tract)
        log_message_ids=["CSCL-LOG-THIN-02", "CSCL-LOG-THIN-03"],
        message_matches={
            "CSCL-LOG-THIN-03": lambda cand: (
                f"HEALTHAREA='' AND BOROUGH = '{cand['borough']}'"
            )
        },
        table="dcp_cscl_atomicpolygons",
        column="censustract_2010",
        new_value=lambda _cand, n: f"99{n % 10000:04d}",
        candidates_sql="""
            SELECT borough || ':' || censustract_2010 AS key, borough, censustract_2010 AS old,
                LIST(globalid ORDER BY globalid) || [borough || censustract_2010] AS claims,
                borough || censustract_2010 AS log_key
            FROM {s}.dcp_cscl_atomicpolygons
            WHERE censustract_2010 IS NOT NULL
            GROUP BY borough, censustract_2010
            HAVING COUNT(*) <= 20
            ORDER BY key
        """,
        key_column="borough:censustract_2010",
        log_match_column="message",
    ),
    MessageKey(
        name="censustract2010_missing_health_area",
        # one ThinLION row for the whole tract (prod's ThinTract file would log TT-02 too,
        # but we don't build it)
        log_message_ids=["CSCL-LOG-THIN-03"],
        table="dcp_cscl_censustract2010",
        column="health_area",
        new_value=lambda _cand, n: f"99{n % 100:02d}",
        candidates_sql="""
            SELECT globalid AS key, [globalid, boroct] AS claims, globalid AS log_key,
                health_area AS old
            FROM {s}.dcp_cscl_censustract2010
            WHERE boroct IN (
                SELECT borough || censustract_2010 FROM {s}.dcp_cscl_atomicpolygons
            )
            ORDER BY globalid
        """,
        log_match_column="message",
    ),
    DeleteNode(
        name="dead_end_node_deleted",
        log_message_ids=["CSCL-LOG-OURS-01"],
        # the segment no longer reads as a dead end, which changes the LION rules'
        # SEGLOCSTATUS when both its sides are the same AtomicPolygon
        may_log_ids=["CSCL-LOG-LION-01"],
        table="dcp_cscl_nodes",
        column="(row deleted)",
        candidates_sql="WITH "
        + _LONE_CENTERLINE
        + ","
        + _LINE_ENDS
        + """,
            ends AS (
                SELECT globalid, segmentid, ROUND(sx, 2) AS x, ROUND(sy, 2) AS y FROM lone
                UNION ALL
                SELECT globalid, segmentid, ROUND(ex, 2), ROUND(ey, 2) FROM lone
            ),
            dead_ends AS (SELECT x, y FROM line_ends WHERE n = 1)
            SELECT n.globalid AS key, [n.globalid, ends.globalid, ends.segmentid::VARCHAR] AS claims,
                ends.globalid AS log_key, n.nodeid::VARCHAR AS old
            FROM ends
            INNER JOIN dead_ends USING (x, y)
            INNER JOIN {s}.dcp_cscl_nodes AS n
                ON ROUND(ST_X(n.geom), 2) = ends.x AND ROUND(ST_Y(n.geom), 2) = ends.y
            ORDER BY n.globalid
        """,
    ),
    Mutation(
        name="proto_orphan_segmentid",
        log_message_ids=["CSCL-LOG-OURS-02"],
        table="dcp_cscl_altsegmentdata",
        column="segmentid",
        new_value=_fake_segmentid,
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, segmentid AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_altsegmentdata
            WHERE alt_segdata_type IS DISTINCT FROM 'S' AND segmentid IS NOT NULL
            ORDER BY globalid
        """,
        dropped_from="lion",
    ),
    # planned ids: these fail recall until their log model lands
    Mutation(
        name="centerline_blank_from_level",
        log_message_ids=["CSCL-LOG-LION-10"],
        table="dcp_cscl_centerline",
        column="from_level_code",
        new_value=None,
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, from_level_code AS old
            FROM {s}.dcp_cscl_centerline WHERE from_level_code IS NOT NULL ORDER BY globalid
        """,
    ),
    Mutation(
        name="centerline_to_level_30",
        log_message_ids=["CSCL-LOG-LION-12"],
        table="dcp_cscl_centerline",
        column="to_level_code",
        new_value=30,
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, to_level_code AS old
            FROM {s}.dcp_cscl_centerline ORDER BY globalid
        """,
    ),
    FlattenCurve(
        name="centerline_straight_curve",
        log_message_ids=["CSCL-LOG-LION-08"],
        table="dcp_cscl_centerline",
        column="curve",
        extra={"sagitta": 0},
        candidates_sql="WITH "
        + _LONE_CENTERLINE
        + """
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, curve AS old, sx, sy, ex, ey
            FROM lone WHERE ST_Length(geom) > 300 ORDER BY globalid
        """,
    ),
    Mutation(
        name="commonplace_orphan_segmentid",
        log_message_ids=["CSCL-LOG-SAF-25"],
        table="dcp_cscl_commonplace_gdb",
        column="segmentid",
        new_value=_fake_segmentid,
        candidates_sql="""
            SELECT globalid AS key, [globalid, placeid::VARCHAR] AS claims,
                globalid AS log_key, segmentid AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_commonplace_gdb
            WHERE saftype IN ('G', 'N', 'X') AND security_level IS DISTINCT FROM 3
            ORDER BY globalid
        """,
        dropped_from="saf",
    ),
    Mutation(
        name="saf_a_orphan_segmentid",
        log_message_ids=["CSCL-LOG-SAF-21"],
        table="dcp_cscl_altsegmentdata",
        column="segmentid",
        new_value=_fake_segmentid,
        candidates_sql="""
            SELECT globalid AS key, [globalid, segmentid::VARCHAR] AS claims,
                globalid AS log_key, segmentid AS old, globalid AS dropped_globalid
            FROM {s}.dcp_cscl_altsegmentdata
            WHERE alt_segdata_type = 'S' AND saftype = 'A'
            ORDER BY globalid
        """,
        dropped_from="saf",
    ),
    # prod's SND query skips NULL lookup keys (LOOKUP_KEY IS NOT NULL) and only logs blank
    # ones; Enders and Exception read ENDERS_FLAG = 'Y' and EXCEPTION_FLAG = 'Y' rows only
    Mutation(
        name="streetname_blank_lookup_key",
        log_message_ids=["CSCL-LOG-SND-01"],
        table="dcp_cscl_streetname",
        column="lookup_key",
        new_value="",
        candidates_sql="""
            SELECT globalid AS key, [globalid, b7sc] AS claims, globalid AS log_key,
                globalid AS dropped_globalid, lookup_key AS old
            FROM {s}.dcp_cscl_streetname
            WHERE principal_flag <> 'Y' AND lookup_key IS NOT NULL AND b10sc IS NOT NULL
                AND enders_flag IS DISTINCT FROM 'Y' AND exception_flag IS DISTINCT FROM 'Y'
            ORDER BY globalid
        """,
        dropped_from="snd",
    ),
    Mutation(
        name="enders_null_lookup_key",
        log_message_ids=["CSCL-LOG-END-01"],
        table="dcp_cscl_streetname",
        column="lookup_key",
        new_value=None,
        candidates_sql="""
            SELECT globalid AS key, [globalid, b7sc] AS claims, globalid AS log_key,
                globalid AS dropped_globalid, lookup_key AS old
            FROM {s}.dcp_cscl_streetname
            WHERE enders_flag = 'Y' AND principal_flag <> 'Y' ORDER BY globalid
        """,
        dropped_from="enders",
    ),
    Mutation(
        name="exception_null_lookup_key",
        log_message_ids=["CSCL-LOG-EXC-02"],
        table="dcp_cscl_streetname",
        column="lookup_key",
        new_value=None,
        candidates_sql="""
            SELECT globalid AS key, [globalid, b7sc] AS claims, globalid AS log_key,
                lookup_key AS old
            FROM {s}.dcp_cscl_streetname
            WHERE exception_flag = 'Y' AND enders_flag IS DISTINCT FROM 'Y'
                AND principal_flag <> 'Y'
            ORDER BY globalid
        """,
    ),
    Mutation(
        name="roadbed_pointer_orphan",
        log_message_ids=["CSCL-LOG-RPL-05"],
        table="dcp_cscl_roadbed_pointer_list",
        column="roadbed_segmentid",
        new_value=_fake_segmentid,
        candidates_sql="""
            SELECT globalid AS key, [globalid, roadbed_segmentid::VARCHAR] AS claims,
                globalid AS log_key, globalid AS dropped_globalid, roadbed_segmentid AS old
            FROM {s}.dcp_cscl_roadbed_pointer_list ORDER BY globalid
        """,
        dropped_from="rpl",
    ),
]


def _exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
            [schema, table],
        ).fetchone()[0]  # type: ignore[index]
    )


def logged_keys(conn: duckdb.DuckDBPyConnection, schema: str) -> set[str]:
    """globalids and record ids already in a baseline log, to keep injections clean."""
    if not _exists(conn, schema, BASELINE_LOG):
        return set()
    rows = conn.execute(
        f"""SELECT globalid FROM "{schema}".{BASELINE_LOG} WHERE globalid IS NOT NULL
        UNION SELECT record_id FROM "{schema}".{BASELINE_LOG} WHERE record_id IS NOT NULL"""
    ).fetchall()
    return {r[0] for r in rows}


def pick(
    candidates: list[dict], rng: random.Random, k: int, used: set[str], logged: set[str]
) -> list[dict]:
    """Up to k random candidates whose claims are all unused and not already logged.

    Marks the picked candidates' claims as used.
    """
    order = list(range(len(candidates)))
    rng.shuffle(order)
    picked: list[dict] = []
    for i in order:
        if len(picked) == k:
            break
        claims = {str(c) for c in candidates[i]["claims"] if c is not None}
        if claims & used or claims & logged:
            continue
        used |= claims
        picked.append(candidates[i])
    return picked


def fetch_candidates(
    conn: duckdb.DuckDBPyConnection, schema: str, m: Mutation
) -> list[dict]:
    cur = conn.execute(m.candidates_sql.format(s=f'"{schema}"'))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def write_manifest(
    conn: duckdb.DuckDBPyConnection, schema: str, injections: list[Injection]
):
    conn.execute(
        f"""CREATE TABLE "{schema}".{MANIFEST} (
            injection_id INT, mutation VARCHAR, log_message_id VARCHAR, expected_ids VARCHAR,
            table_name VARCHAR, key_column VARCHAR, key_value VARCHAR, column_name VARCHAR,
            old_value VARCHAR, new_value VARCHAR, log_match_column VARCHAR,
            log_match_value VARCHAR, dropped_from VARCHAR, dropped_globalid VARCHAR,
            required BOOLEAN
        )"""
    )
    rows = [
        (
            i,
            inj.mutation,
            log_id,
            ",".join(inj.log_message_ids),
            inj.table_name,
            inj.key_column,
            inj.key_value,
            inj.column_name,
            inj.old_value,
            inj.new_value,
            "message" if log_id in inj.message_matches else inj.log_match_column,
            inj.message_matches.get(log_id, inj.log_match_value),
            inj.dropped_from,
            inj.dropped_globalid,
            required,
        )
        for i, inj in enumerate(injections, start=1)
        for log_id, required in [(x, True) for x in inj.log_message_ids]
        + [(x, False) for x in inj.may_log_ids]
    ]
    conn.executemany(
        f'INSERT INTO "{schema}".{MANIFEST} VALUES ({", ".join("?" * 15)})', rows
    )


def inject(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    seed: int,
    max_per_check: int,
    ids: set[str] | None = None,
) -> list[Injection]:
    if _exists(conn, schema, MANIFEST):
        raise SystemExit(
            f"{schema}.{MANIFEST} exists: start again from a fresh inputs copy"
        )
    # log_records: log.csv plus its attachments' records (log.csv alone on older builds)
    log_table = next(
        (t for t in ("log_records", "log") if _exists(conn, schema, t)), None
    )
    if log_table:
        conn.execute(
            f'CREATE OR REPLACE TABLE "{schema}".{BASELINE_LOG} AS SELECT * FROM "{schema}".{log_table}'
        )
    logged = logged_keys(conn, schema)
    rng = random.Random(seed)
    used: set[str] = set()
    injections: list[Injection] = []
    for m in MUTATIONS:
        if ids and not ids & set(m.log_message_ids):
            continue
        k = rng.randint(1, max_per_check)
        cands = fetch_candidates(conn, schema, m)
        chosen = pick(cands, rng, k, used, logged)
        if len(chosen) < k:
            print(f"{m.name}: only {len(chosen)} of {k} clean candidates")
        for cand in chosen:
            injections.append(m.apply(conn, schema, cand, len(injections) + 1))
    write_manifest(conn, schema, injections)
    return injections


def report(conn: duckdb.DuckDBPyConnection, schema: str) -> None:
    rows = conn.execute(
        f'SELECT * FROM "{schema}".qa__chaos_summary ORDER BY 1'
    ).fetchall()
    cols = [
        d[0]
        for d in conn.execute(
            f'SELECT * FROM "{schema}".qa__chaos_summary LIMIT 0'
        ).description
    ]
    print("\t".join(cols))
    for r in rows:
        print("\t".join("" if v is None else str(v) for v in r))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Break loaded CSCL inputs so log.csv should catch each one."
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--max-per-check", type=int, default=2, help="1..k records per mutation"
    )
    parser.add_argument("--ids", help="comma-separated log_message_ids to restrict to")
    parser.add_argument("--report", action="store_true", help="print qa__chaos_summary")
    args = parser.parse_args()

    schema = os.environ["BUILD_ENGINE_SCHEMA"]
    conn = duckdb.connect(os.environ["DUCKDB_PATH"])
    conn.execute("LOAD spatial")
    if args.report:
        report(conn, schema)
        return
    ids = set(args.ids.split(",")) if args.ids else None
    injections = inject(conn, schema, args.seed, args.max_per_check, ids)
    for inj in injections:
        print(
            f"{','.join(inj.log_message_ids)}\t{inj.table_name}.{inj.column_name}\t"
            f"{inj.key_column}={inj.key_value}\t{inj.old_value!r} -> {inj.new_value!r}"
        )
    print(f"{len(injections)} injections written to {schema}.{MANIFEST}")


if __name__ == "__main__":
    main()
