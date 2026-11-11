import logging

import pytest

from dcpy.utils.csv_repair import UnescapedDelimiterRepair, make_bad_line_handler

HEADER = ["id", "name", "street", "borough", "block", "description", "filed"]
REPAIR = UnescapedDelimiterRepair(
    free_text_columns=["name", "street", "description"],
    anchor_columns={"borough": "borough", "block": "digits", "filed": "iso_date"},
)


@pytest.fixture
def handler():
    return make_bad_line_handler(HEADER, REPAIR, "|")


def test_repairs_last_free_text_column(handler, caplog):
    line = ["1", "ACME", "MAIN ST", "BROOKLYN", "12", "a ", " b", "2026-01-01"]
    with caplog.at_level(logging.WARNING):
        assert handler(line) == [
            "1", "ACME", "MAIN ST", "BROOKLYN", "12", "a | b", "2026-01-01"
        ]  # fmt: skip
    assert "'description'" in caplog.text


def test_adjacent_candidates_pick_the_first(handler):
    # "name" and "street" both produce a valid merge. Every anchor is the same either way.
    line = ["2", "AB ", " CD", "MAIN ST", "MANHATTAN", "3", "x", "2026-01-01"]
    assert handler(line) == [
        "2", "AB | CD", "MAIN ST", "MANHATTAN", "3", "x", "2026-01-01"
    ]  # fmt: skip


def test_drops_row_when_no_merge_is_valid(handler, caplog):
    line = ["3", "ACME", "MAIN ST", "BROOKLYN", "1a", "b2", "x", "2026-01-01"]
    with caplog.at_level(logging.WARNING):
        assert handler(line) is None
    assert "Dropping row '3'" in caplog.text


def test_drops_row_when_valid_merges_are_far_apart(caplog):
    repair = REPAIR.model_copy(update={"max_candidate_spread": 0})
    handler = make_bad_line_handler(HEADER, repair, "|")
    # merging into "name" or "street" both validate, one column apart
    line = ["4", "A", "B", "C", "BROOKLYN", "5", "x", "2026-01-01"]
    with caplog.at_level(logging.WARNING):
        assert make_bad_line_handler(HEADER, REPAIR, "|")(line) is not None
        assert handler(line) is None
    assert "unrelated columns" in caplog.text


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="not_a_column"):
        make_bad_line_handler(
            HEADER,
            REPAIR.model_copy(update={"free_text_columns": ["not_a_column"]}),
            "|",
        )
