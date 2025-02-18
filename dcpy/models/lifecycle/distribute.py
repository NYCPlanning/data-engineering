from dataclasses import dataclass
from typing import TypedDict, NotRequired, Required
from pathlib import Path

import dcpy.models.product.dataset.metadata as ds_md


class DatasetDestinationFilters(TypedDict):
    datasets: NotRequired[frozenset[str]]
    destination_tag: NotRequired[str]
    destination_id: NotRequired[str]
    destination_type: NotRequired[str]


class DatasetDestinationPushArgs(TypedDict):
    metadata: Required[ds_md.Metadata]
    dataset_destination_id: Required[str]
    publish: NotRequired[bool]
    dataset_package_path: NotRequired[Path]
    metadata_only: NotRequired[bool]


@dataclass
class DistributeResult:
    dataset_id: str
    destination_id: str
    destination_type: str

    result: str | None = None
    success: bool | None = None

    @staticmethod
    def from_push_kwargs(
        result: str, success: bool, push_args: DatasetDestinationPushArgs
    ):
        """Convenience static factory to create the this class from push args,
        since we'll probably have them handy whenever instantiating this class."""
        ds_md = push_args["metadata"]
        dest = ds_md.get_destination(push_args["dataset_destination_id"])
        return DistributeResult(
            result=result,
            success=success,
            dataset_id=ds_md.id,
            destination_type=dest.type,
            destination_id=dest.id,
        )
