import logging

from dcpy.library.script import dob_now_applications

HEADER = [
    "Job Filing Number",
    "Borough",
    "Block",
    "LOT",
    "Bin",
    "Owner's Business Name",
    "State",
    "Zip",
    "Filing Representative State",
    "Filing Representative Zip",
    "Latitude",
    "Longitude",
    "Filing Date",
    "JobType",
    "JobDescription",
    "SpecialDistrict1",
]


def test_repairs_row_with_embedded_pipe_in_job_description(caplog):
    # "some | description text" typed into JobDescription; a naive split on
    # "|" breaks it into two fields.
    bad_line = [
        "Q1",
        "MANHATTAN",
        "100",
        "1",
        "1000000",
        "",
        "NY",
        "10001",
        "NY",
        "10001",
        "40.7",
        "-73.9",
        "2026-01-01",
        "Alteration",
        "some ",
        " description text",
        "SD1",
    ]
    handler = dob_now_applications._make_bad_line_handler(HEADER)

    with caplog.at_level(logging.WARNING):
        result = handler(bad_line)

    assert result == [
        "Q1",
        "MANHATTAN",
        "100",
        "1",
        "1000000",
        "",
        "NY",
        "10001",
        "NY",
        "10001",
        "40.7",
        "-73.9",
        "2026-01-01",
        "Alteration",
        "some | description text",
        "SD1",
    ]
    assert "Repaired" in caplog.text
    assert "JobDescription" in caplog.text


def test_repairs_row_with_embedded_pipe_in_a_different_free_text_column(caplog):
    # Same bug, but the stray pipe is in Owner's Business Name instead -
    # proves the repair isn't hardcoded to a single column.
    bad_line = [
        "Q2",
        "BROOKLYN",
        "200",
        "2",
        "2000000",
        "ABC ",
        " DEF INC",
        "NY",
        "11201",
        "NY",
        "11201",
        "40.6",
        "-73.8",
        "2026-02-02",
        "Demolition",
        "clean description",
        "SD2",
    ]
    handler = dob_now_applications._make_bad_line_handler(HEADER)

    with caplog.at_level(logging.WARNING):
        result = handler(bad_line)

    assert result == [
        "Q2",
        "BROOKLYN",
        "200",
        "2",
        "2000000",
        "ABC | DEF INC",
        "NY",
        "11201",
        "NY",
        "11201",
        "40.6",
        "-73.8",
        "2026-02-02",
        "Demolition",
        "clean description",
        "SD2",
    ]
    assert "Repaired" in caplog.text
    assert "Owner's Business Name" in caplog.text


def test_drops_row_when_no_free_text_column_repair_is_plausible(caplog):
    # Extra field lands between two anchors (Block/LOT) that no free-text
    # candidate merge could ever fix - nothing validates, so drop.
    bad_line = [
        "Q3",
        "MANHATTAN",
        "1a",
        "b2",
        "1",
        "1000000",
        "",
        "NY",
        "10001",
        "NY",
        "10001",
        "40.7",
        "-73.9",
        "2026-01-01",
        "Alteration",
        "clean description",
        "SD1",
    ]
    handler = dob_now_applications._make_bad_line_handler(HEADER)

    with caplog.at_level(logging.WARNING):
        result = handler(bad_line)

    assert result is None
    assert "Dropping" in caplog.text
    assert "Q3" in caplog.text


def test_drops_row_with_too_few_fields(caplog):
    bad_line = ["Q4", "MANHATTAN", "100"]
    handler = dob_now_applications._make_bad_line_handler(HEADER)

    with caplog.at_level(logging.WARNING):
        result = handler(bad_line)

    assert result is None
    assert "Dropping" in caplog.text
