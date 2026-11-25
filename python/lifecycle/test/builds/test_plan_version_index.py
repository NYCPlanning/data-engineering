"""plan_recipe with a VersionIndex: inputs it answers skip the bucket, the rest don't."""

from datetime import date
from unittest import mock

import pytest

from dcpy.connectors.edm.models import DatasetType
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds.version_index import InputRequest, ResolvedInput

RECIPE = """
name: Tester
version: 23v1
product: db-test
env:
  VERSION_PREV: 22v1
inputs:
  missing_versions_strategy: find_latest
  datasets:
  - name: indexed
  - name: pinned
    version: v1
  - name: unknown
"""

INDEXED = ResolvedInput(
    version="v2",
    file_type=DatasetType.parquet,
    name="indexed name",
    archive_date=date(2026, 11, 1),
    url="https://example.com/indexed",
)


class FakeIndex:
    def __init__(self, answers=None, error=None):
        self.answers = answers or {}
        self.error = error
        self.calls: list = []

    def resolve(self, source, inputs, file_type_preference):
        self.calls.append((source, list(inputs)))
        if self.error:
            raise self.error
        return [self.answers.get(i) for i in inputs]


@pytest.fixture
def recipe_path(tmp_path):
    path = tmp_path / "recipe.yml"
    path.write_text(RECIPE)
    return path


@pytest.fixture
def bucket():
    """The bucket path, faked: records which inputs went to it."""
    connector = mock.Mock(conn_type="edm.recipes.datasets")
    connector.get_latest_version.side_effect = lambda key: f"{key}-latest"
    connector.get_name.side_effect = lambda key, version: f"{key} from bucket"
    with (
        mock.patch.object(plan, "connectors", {"edm.recipes.datasets": connector}),
        mock.patch.object(
            plan.recipes, "get_preferred_file_type", return_value=DatasetType.csv
        ) as file_type,
        mock.patch.object(plan.recipes, "get_archive_date", return_value=None),
        mock.patch.object(plan.recipes, "get_url", return_value=None),
    ):
        yield file_type


def _by_id(recipe):
    return {ds.id: ds for ds in recipe.inputs.datasets}


def test_index_answers_skip_the_bucket(recipe_path, bucket):
    index = FakeIndex({InputRequest("indexed", "latest"): INDEXED})
    planned = _by_id(plan.plan_recipe(recipe_path, version_index=index))

    assert index.calls == [
        (
            "edm.recipes.datasets",
            [
                InputRequest("indexed", "latest"),
                InputRequest("pinned", "v1"),
                InputRequest("unknown", "latest"),
            ],
        )
    ]
    indexed = planned["indexed"]
    assert (indexed.version, indexed.file_type, indexed.name, indexed.archive_date) == (
        "v2",
        DatasetType.parquet,
        "indexed name",
        date(2026, 11, 1),
    )
    assert planned["pinned"].version == "v1"
    assert planned["unknown"].version == "unknown-latest"
    assert {c.args[0].id for c in bucket.call_args_list} == {"pinned", "unknown"}


@pytest.mark.parametrize("index", [None, FakeIndex(error=OSError("down"))])
def test_no_index_or_failing_index_reads_the_bucket(recipe_path, bucket, index):
    planned = _by_id(plan.plan_recipe(recipe_path, version_index=index))
    assert planned["indexed"].version == "indexed-latest"
    assert planned["indexed"].name == "indexed from bucket"
    assert {c.args[0].id for c in bucket.call_args_list} == {
        "indexed",
        "pinned",
        "unknown",
    }
