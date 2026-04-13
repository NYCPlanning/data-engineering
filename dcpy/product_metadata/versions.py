from pathlib import Path

from pydantic import BaseModel

from dcpy.product_metadata.keys import DestinationKey


class DestinationVersion(BaseModel):
    """Single destination with its version."""

    key: DestinationKey
    version: str

    @classmethod
    def from_path(cls, destination_path: str, version: str) -> "DestinationVersion":
        """Parse product.dataset.destination_id and create instance."""
        return cls(
            key=DestinationKey.from_path_str(destination_path),
            version=version,
        )


class VersionChange(BaseModel):
    """Represents a version change for a destination."""

    key: DestinationKey
    old_version: str
    new_version: str


class VersionDiff(BaseModel):
    """Comparison between two DestinationVersions instances."""

    added: list[DestinationVersion] = []
    changed: list[VersionChange] = []
    removed: list[DestinationVersion] = []

    @property
    def summary(self) -> dict[str, int]:
        return {
            "added_count": len(self.added),
            "changed_count": len(self.changed),
            "removed_count": len(self.removed),
        }

    def to_text(self) -> str:
        """Generate human-readable text output."""
        lines = []

        if self.added:
            lines.append("## Added")
            lines.extend([f"+ {a.key.path_str}|{a.version}" for a in self.added])
            lines.append("")

        if self.changed:
            lines.append("## Changed")
            lines.extend(
                [
                    f"  {c.key.path_str}|{c.old_version} -> {c.new_version}"
                    for c in self.changed
                ]
            )
            lines.append("")

        if self.removed:
            lines.append("## Removed")
            lines.extend([f"- {r.key.path_str}|{r.version}" for r in self.removed])
            lines.append("")

        if not (self.added or self.changed or self.removed):
            lines.append("No changes detected.")

        return "\n".join(lines)


class DestinationVersions(BaseModel):
    """Collection of destination versions with comparison capabilities."""

    # Use dict for efficient lookup: {"product.dataset.destination": "version"}
    versions: dict[str, str]

    @classmethod
    def from_org_metadata(cls, org_metadata) -> "DestinationVersions":
        """Build from OrgMetadata instance."""
        version_list = org_metadata.get_all_destination_current_versions()
        versions_dict = {}
        for line in version_list:
            if "|" in line:
                dest, ver = line.split("|", 1)
                versions_dict[dest] = ver
        return cls(versions=versions_dict)

    @classmethod
    def from_json_file(cls, path: Path) -> "DestinationVersions":
        """Load from JSON file."""
        import json

        return cls(versions=json.loads(path.read_text()))

    def to_json_file(self, path: Path) -> None:
        """Write to JSON file."""
        import json

        path.write_text(json.dumps(self.versions, indent=2) + "\n")

    def to_json_string(self) -> str:
        """Export as JSON string."""
        import json

        return json.dumps(self.versions, indent=2)

    def compare(self, other: "DestinationVersions") -> VersionDiff:
        """Compare this version snapshot to another.

        Args:
            other: The newer version to compare against

        Returns:
            VersionDiff with added, changed, and removed destinations
        """
        added = []
        changed = []
        removed = []

        # Find added and changed
        for dest, new_ver in sorted(other.versions.items()):
            if dest not in self.versions:
                added.append(DestinationVersion.from_path(dest, new_ver))
            elif self.versions[dest] != new_ver:
                changed.append(
                    VersionChange(
                        key=DestinationKey.from_path_str(dest),
                        old_version=self.versions[dest],
                        new_version=new_ver,
                    )
                )

        # Find removed
        for dest, old_ver in sorted(self.versions.items()):
            if dest not in other.versions:
                removed.append(DestinationVersion.from_path(dest, old_ver))

        return VersionDiff(added=added, changed=changed, removed=removed)
