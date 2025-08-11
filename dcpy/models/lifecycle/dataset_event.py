from pathlib import Path

from tabulate import tabulate
from dcpy.models.base import SortedSerializedBase


class LifecycleDatasetResult(SortedSerializedBase):
    product: str
    dataset: str
    version: str
    success: bool

    result_summary: str | None = None
    result_details: str | None = None

    def other_identifying_info(self) -> str:
        return ""


class DistributeResult(LifecycleDatasetResult):
    destination_id: str

    def other_identifying_info(self) -> str:
        return self.destination_id


class PackageAssembleResult(LifecycleDatasetResult):
    source_id: str
    package_path: Path | None = None
    validation_errors: list[tuple] = []

    def other_identifying_info(self) -> str:
        return self.source_id


def make_results_table(distribute_results: list[LifecycleDatasetResult]) -> str:
    return tabulate(
        [
            [
                f"{r.product}.{r.dataset}:{r.version}",
                r.other_identifying_info(),
                "✅" if r.success else "❌",
                f"{r.result_details} - {r.result_details}",
            ]
            for r in distribute_results
        ],
        headers=["dataset", "source/dest id", "success?", "result"],
        tablefmt="presto",
        maxcolwidths=[20, 10, 8, 70],
    )
