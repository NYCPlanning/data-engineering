from dagster import DynamicPartitionsDefinition


def get_build_partition_def(product_name: str) -> DynamicPartitionsDefinition:
    """Get or create a DynamicPartitionsDefinition for a specific product.

    Each product gets its own partition definition to track versions independently.

    Args:
        product_name: Name of the product (e.g., "edde", "pluto")

    Returns:
        DynamicPartitionsDefinition for this product
    """
    return DynamicPartitionsDefinition(name=f"build_{product_name}")
