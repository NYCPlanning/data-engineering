from datetime import datetime
import pytest

from dcpy.models.lifecycle.ingest import SparseConfig


class TestIngestSparseConfig:
    ds = "TEST"
    version = "TEST_VERSION"
    timestamp = datetime.now()
    timestamp_str = timestamp.isoformat()

    @pytest.mark.parametrize(
        "input, expected",
        [
            (  # ingest
                {
                    "id": ds,
                    "version": version,
                    "transformation": {"run_details": {"timestamp": timestamp_str}},
                },
                {"id": ds, "version": version, "run_timestamp": timestamp},
            ),
            (  # ingest raw
                {"id": ds, "timestamp": timestamp_str},
                {"id": ds, "version": timestamp_str, "run_timestamp": timestamp},
            ),
            (  # legacy ingest
                {
                    "id": ds,
                    "version": version,
                    "run_details": {"timestamp": timestamp_str},
                },
                {"id": ds, "version": version, "run_timestamp": timestamp},
            ),
            (  # library
                {
                    "dataset": {
                        "name": ds,
                        "version": version,
                    },
                    "extraction_details": {"timestamp": timestamp_str},
                },
                {"id": ds, "version": version, "run_timestamp": timestamp},
            ),
        ],
    )
    def test_ingest_sparse_config(self, input, expected):
        config = SparseConfig(**input)
        assert config.model_dump() == expected
