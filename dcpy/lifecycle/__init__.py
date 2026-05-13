"""Lifecycle module for data engineering operations."""

from typing import List

from dcpy.configuration import PRODUCTS_DIR
from dcpy.lifecycle.asset_models import Product


def list_products() -> List[Product]:
    """List all available products.

    Returns:
        List of Product objects with name and path attributes

    Raises:
        FileNotFoundError: If the products directory doesn't exist
    """
    products_dir = PRODUCTS_DIR

    if not products_dir.exists():
        raise FileNotFoundError(
            f"Products directory not found at {products_dir}. "
            f"Set PRODUCTS_DIR environment variable to the correct path."
        )

    products = []
    for product_dir in sorted(products_dir.iterdir()):
        if product_dir.is_dir():
            recipe_file = product_dir / "recipe.yml"
            if recipe_file.exists():
                products.append(
                    Product(
                        name=product_dir.name,
                        path=product_dir,
                    )
                )

    return products


__all__ = ["list_products"]
