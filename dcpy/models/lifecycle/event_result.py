from pathlib import Path

from tabulate import tabulate  # type: ignore

from dcpy.models.base import SortedSerializedBase


class LifecycleDatasetResult(SortedSerializedBase):
    product: str
    dataset: str
    version: str
    success: bool

    result_summary: str | None = None
    result_details: str | None = None


class DistributeResult(LifecycleDatasetResult):
    destination_id: str
    local_package_path: Path | None = None


class PackageAssembleResult(LifecycleDatasetResult):
    source_id: str
    package_path: Path | None = None
    validation_errors: list[tuple] = []


def make_results_table(distribute_results: list[DistributeResult]) -> str:
    return tabulate(
        [
            [
                r.destination_id,
                f"{r.product}.{r.dataset}:{r.version}",
                "✅" if r.success else "❌",
                f"{r.result_summary} - {r.result_details}",
            ]
            for r in distribute_results
        ],
        headers=["destination", "dataset", "success?", "result"],
        tablefmt="presto",
        maxcolwidths=[50, 50, 8, 100],
    )
