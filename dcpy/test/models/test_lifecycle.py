from datetime import datetime

import pytest

from dcpy.models.lifecycle.ingest import SparseConfig


class TestIngestSparseConfig:
    ds = "TEST"
    version = "TEST_VERSION"
    name = "DCP Test Dataset"
    timestamp = datetime.now()
    timestamp_str = timestamp.isoformat()

    @pytest.mark.parametrize(
        argnames="input, expected",
        argvalues=[
            pytest.param(
                {
                    "id": ds,
                    "version": version,
                    "attributes": {"name": name},
                    "transformation": {"run_details": {"timestamp": timestamp_str}},
                },
                {
                    "id": ds,
                    "version": version,
                    "name": name,
                    "run_timestamp": timestamp,
                },
                id="ingest",
            ),
            pytest.param(
                {"id": ds, "attributes": {"name": name}, "timestamp": timestamp_str},
                {
                    "id": ds,
                    "name": name,
                    "version": timestamp_str,
                    "run_timestamp": timestamp,
                },
                id="ingest raw",
            ),
            pytest.param(
                {
                    "id": ds,
                    "version": version,
                    "attributes": {"name": name},
                    "run_details": {"timestamp": timestamp_str},
                },
                {
                    "id": ds,
                    "name": name,
                    "version": version,
                    "run_timestamp": timestamp,
                },
                id="legacy ingest",
            ),
            pytest.param(
                {
                    "dataset": {
                        "name": ds,
                        "version": version,
                    },
                    "extraction_details": {"timestamp": timestamp_str},
                },
                {
                    "id": ds,
                    "version": version,
                    "name": None,
                    "run_timestamp": None,
                    "extraction_details": {"timestamp": timestamp_str},
                },
                id="library",
            ),
        ],
    )
    def test_ingest_sparse_config(self, input, expected):
        config = SparseConfig(**input)
        assert config.model_dump() == expected
