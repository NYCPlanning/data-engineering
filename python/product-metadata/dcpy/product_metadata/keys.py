from pydantic import BaseModel


class DestinationKey(BaseModel):
    """Parsed destination identifier: product.dataset.destination_id."""

    product: str
    dataset: str
    destination_id: str

    @classmethod
    def from_path_str(cls, path: str) -> "DestinationKey":
        """Parse product.dataset.destination_id string into components.

        Args:
            path: Destination path like "product.dataset.destination_id"

        Returns:
            DestinationKey instance
        """
        parts = path.split(".")
        return cls(
            product=parts[0] if len(parts) >= 1 else "",
            dataset=parts[1] if len(parts) >= 2 else "",
            destination_id=parts[2] if len(parts) >= 3 else "",
        )

    @property
    def path_str(self) -> str:
        """Return the full path: product.dataset.destination_id."""
        return f"{self.product}.{self.dataset}.{self.destination_id}"

    def __str__(self) -> str:
        return self.path_str
