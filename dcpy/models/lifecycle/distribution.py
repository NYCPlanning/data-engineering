from typing import TypedDict, NotRequired, Required
from pathlib import Path

import dcpy.models.product.dataset.metadata as ds_md


class PublisherPushKwargs(TypedDict):
    metadata: Required[ds_md.Metadata]
    dataset_destination_id: Required[str]
    publish: NotRequired[bool]
    dataset_package_path: NotRequired[Path]
    metadata_only: NotRequired[bool]
