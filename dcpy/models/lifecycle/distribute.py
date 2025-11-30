from pathlib import Path
from typing import NotRequired, Required, TypedDict

import dcpy.models.product.dataset.metadata as ds_md


class DatasetDestinationPushArgs(TypedDict):
    metadata: Required[ds_md.Metadata]
    dataset_destination_id: Required[str]
    publish: NotRequired[bool]
    dataset_package_path: NotRequired[Path]
    metadata_only: NotRequired[bool]
