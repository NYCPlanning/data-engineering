from pathlib import Path
import pytest
from unittest import mock

from dcpy.lifecycle.ingest import extract

from .shared import RUN_DETAILS, RESOLVED, ARCHIVED


def test_extract(tmp_path):
    result = extract.extract_source(RESOLVED, RUN_DETAILS, tmp_path)
    assert (tmp_path / result.raw_filename).exists()
    assert result == ARCHIVED


def test_extract_invalid_source():
    with pytest.raises(Exception, match="No registered connector"):
        extract.extract_source(
            mock.MagicMock(source=mock.MagicMock(type="fake")), RUN_DETAILS, Path(".")
        )
