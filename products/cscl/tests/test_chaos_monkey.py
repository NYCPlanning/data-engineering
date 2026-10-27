"""chaos_monkey: the registry matches the log_messages seed, and record selection."""

import csv
import random
import sys
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

import chaos_monkey as cm  # noqa: E402

SEED = Path(__file__).parent.parent / "seeds" / "log_messages.csv"


def _seed_rows() -> dict[str, dict]:
    with open(SEED, newline="") as f:
        return {row["id"]: row for row in csv.DictReader(f)}


def test_registry_ids_are_in_the_seed_and_injectable():
    rows = _seed_rows()
    for m in cm.MUTATIONS:
        for log_id in m.log_message_ids:
            assert log_id in rows, f"{m.name}: {log_id} is not in log_messages.csv"
            assert rows[log_id]["status"] in ("implemented", "planned"), (
                m.name,
                log_id,
            )


def test_seed_chaos_mutations_column_matches_registry():
    expected: dict[str, list[str]] = {}
    for m in cm.MUTATIONS:
        for log_id in m.log_message_ids:
            expected.setdefault(log_id, []).append(m.name)
    for log_id, row in _seed_rows().items():
        assert row["chaos_mutations"] == " ".join(expected.get(log_id, [])), log_id


def test_mutation_names_are_unique():
    names = [m.name for m in cm.MUTATIONS]
    assert len(names) == len(set(names))


def test_pick_skips_used_and_logged_claims():
    cands = [
        {"key": "a", "claims": ["g1", "10"]},
        {"key": "b", "claims": ["g2", "10"]},  # shares segment 10 with a
        {"key": "c", "claims": ["g3", "30"]},  # already in the baseline log
        {"key": "d", "claims": ["g4", "40"]},
    ]
    used: set[str] = set()
    picked = cm.pick(cands, random.Random(0), 3, used, logged={"g3"})
    keys = {c["key"] for c in picked}
    assert "c" not in keys
    assert not {"a", "b"} <= keys
    assert len(picked) == 2
    assert used == {claim for c in picked for claim in c["claims"]}


def test_pick_is_reproducible():
    cands = [{"key": str(i), "claims": [str(i)]} for i in range(50)]
    first = cm.pick(cands, random.Random(7), 5, set(), set())
    again = cm.pick(cands, random.Random(7), 5, set(), set())
    assert first == again


def test_inject_writes_manifest_and_edits_records():
    conn = duckdb.connect()
    conn.execute("create schema s")
    conn.execute(
        "create table s.dcp_cscl_nonstreetfeatures as select * from (values "
        "('{A}', 1, 3::SMALLINT), ('{B}', 2, 1::SMALLINT), ('{C}', 3, 6::SMALLINT)) "
        "t(globalid, segmentid, linetype)"
    )
    # {B} is already logged in the baseline, so it is never picked
    conn.execute(
        "create table s.log as select 'CSCL-LOG-LION-23' as log_message_id, '{B}' as globalid, "
        "null as record_id, 'x' as message"
    )
    injections = cm.inject(conn, "s", seed=1, max_per_check=5, ids={"CSCL-LOG-LION-23"})

    picked = {i.key_value for i in injections}
    assert picked and picked <= {"{A}", "{C}"}
    old = {"{A}": 3, "{B}": 1, "{C}": 6}
    assert conn.execute(
        "select globalid, linetype from s.dcp_cscl_nonstreetfeatures order by 1"
    ).fetchall() == [(g, 7 if g in picked else v) for g, v in old.items()]
    assert conn.execute(
        "select log_message_id, log_match_value, old_value, new_value "
        "from s.chaos_injections order by log_match_value"
    ).fetchall() == [("CSCL-LOG-LION-23", g, str(old[g]), "7") for g in sorted(picked)]
    assert conn.execute("select count(*) from s.chaos_baseline_log").fetchone() == (1,)


def test_rebuild_selection_covers_edited_tables_and_chaos_models():
    sel = cm.rebuild_selection({"dcp_cscl_nodes", "dcp_cscl_centerline"})
    assert sel == [
        "source:recipe_sources.dcp_cscl_centerline+,+qa__chaos_summary",
        "source:recipe_sources.dcp_cscl_nodes+,+qa__chaos_summary",
        "qa__chaos_injections",
        "qa__chaos_summary",
    ]


def test_every_mutation_edits_a_declared_source():
    sources_yml = Path(__file__).parent.parent / "models" / "sources.yml"
    sources = yaml.safe_load(sources_yml.read_text())["sources"]
    (source,) = [s for s in sources if s["name"] == cm.SOURCE]
    tables = {t["name"] for t in source["tables"]}
    for m in cm.MUTATIONS:
        assert m.table in tables, m.name


def test_edited_tables_include_every_table_an_edit_touches():
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA s")
    conn.execute("CREATE TABLE s.a (globalid VARCHAR, x VARCHAR)")
    conn.execute("CREATE TABLE s.b (globalid VARCHAR, y VARCHAR)")
    conn.execute(
        "INSERT INTO s.a VALUES ('g1', 'old'); INSERT INTO s.b VALUES ('g2', 'old')"
    )
    m = cm.Edits(name="m", log_message_ids=[], table="a", column="x", candidates_sql="")
    cand = {
        "key": "g1",
        "old": "old",
        "log_key": "g1",
        "edits": [
            {"t": "a", "k": "globalid", "kv": "g1", "c": "x", "v": "new"},
            {"t": "b", "k": "globalid", "kv": "g2", "c": "y", "v": "new"},
        ],
    }
    inj = m.apply(conn, "s", cand, 1)
    assert cm.edited_tables([inj]) == {"a", "b"}
    assert conn.execute("SELECT y FROM s.b").fetchone() == ("new",)
