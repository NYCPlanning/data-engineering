from pathlib import Path

from dcpy.models.product.metadata import OrgMetadata


def write_yaml(
    *,
    org_metadata: OrgMetadata,
    product: str,
    output_path: Path,
    dataset: str | None = None,
) -> None:
    if not dataset:
        dataset = product

    metadata = org_metadata.product(product).dataset(dataset)
    metadata.write_to_yaml(output_path)
