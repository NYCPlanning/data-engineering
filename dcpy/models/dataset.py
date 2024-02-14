"""FYI, where possible, I tried to use DCAT standards for naming
https://www.w3.org/TR/vocab-dcat-3/#Property:catalog_dataset

Overall:
- New bucket for comparisons `edm-dataset-comparisons`
- Store comparisons as f"{product_name}/{dataset1_build_name}/{dataset2_build_name}/{comparison_name}_v{comparison_version}"
-- maybe we want
-- Could also store the comparisons as JSON in postgres, for easy querying in QA app
-- Or... just store this is edm-publishing (for datasets), and recipes for recipes

- would replace the long-running appended spreadsheet, and add the ability to re-run comparisons for entire dataset series,
  or run new comparisons


Comparison Code:
- dcpy: a shared set of QA comparisons.
--      For example, the comparisons that show how some specified column has changed from version-to-version
- Most comparison Scripts / Validations would live in the product still.
- new dcpy connector to upload / cache (in pg) / query those comparisons

POTENTIAL PROBLEMS:
- Scenario: Datasets are overwritten, but comparisons are not re-run.
-- Dataset immutability would help here. Perhaps we should start appending a timestamp to the end of a build name.

OTHER CONSIDERATIONS
- All datasets should have a key / id / unique_name. Some sort of unique lookup value. We'll need this for metadata work, in any case.
- Drafts are deleted... do we just not do comparisons on those? or do we also prune them?

"""


from dataclasses import dataclass
from pathlib import Path
import typing
from typing import TypedDict


class Dataset:
    """Could be a recipe, or a product(draft or published). Maybe a distribution?"""

    key: str
    ds_type: str  # Could be recipe
    version: str
    inputs: list[str]

    def download(self):
        match self.ds_type:
            case "recipe":
                pass
            case "publishing":
                pass
            case "publishing_draft":
                pass


@dataclass
class Catalog:
    datasets: typing.dict[str, Dataset]


class DatasetSeriesFilters(TypedDict):
    version_type: str | None  # e.g Major or Minor

    # tbd, just need something to limit the results so we don't
    # kick off an endless number of comparisons
    version_gte: str


class DatasetSeries:
    """Could represent any type of dataset.
    e.g. all published versions of a dataset. Or all major versions, etc.
    We could use this to generate comparisons for groups of datasets.
    """

    datasets: list[Dataset]

    def __init__(self, dataset_id, dataset_type, *, filters: DatasetSeriesFilters):
        match dataset_type:
            case "recipe":
                self.datasets = []  # recipes.query(...)
            case "publishing":
                self.datasets = []  # publishing.query(...)


@dataclass
class DatasetQAScript:
    name: str
    path: Path


@dataclass
class DatasetQAModule:
    name: str
    module: str
    function: str


@dataclass
class DatasetComparisonOutput:
    name: str
    ds1: Dataset
    ds2: Dataset
    comparison: DatasetQAScript | DatasetQAModule
