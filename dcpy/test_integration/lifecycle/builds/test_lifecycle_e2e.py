import json
from datetime import datetime

import pytest
import pytz

from dcpy.lifecycle.builds.artifacts import builds, drafts, published
from dcpy.models.lifecycle.builds import BuildMetadata, Recipe, RecipeInputs


@pytest.mark.incremental
class TestLifecycleE2E:
    """Test the complete build lifecycle in sequence."""

    @classmethod
    def setup_class(cls):
        """Initialize test data shared across all test methods."""
        cls.product = "db-mock-product"
        cls.build_name = "mock-build"
        cls.version = "24v3"

    def test_build_upload(self, setup_local_connectors, tmp_path):
        """Test creating and uploading a build."""
        build_path = tmp_path

        # Create mock build metadata
        recipe_inputs = RecipeInputs(datasets=[])
        recipe = Recipe(
            name="mock-recipe",
            product=self.product,
            version=self.version,
            inputs=recipe_inputs,
        )

        metadata = BuildMetadata(
            version=self.version,
            timestamp=datetime.now(pytz.timezone("America/New_York")).isoformat(),
            recipe=recipe,
        )

        metadata_file = build_path / "build_metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata.model_dump(mode="json"), f, indent=4)

        # Create a simple data file
        data_file = build_path / "data.txt"
        data_file.write_text("mock data content")

        # Upload to builds
        build_key = builds.upload(
            output_path=build_path, product=self.product, build=self.build_name
        )

        assert build_key.product == self.product
        assert build_key.build == self.build_name

        # Query the uploaded build to verify it exists
        build_metadata = builds.get_metadata(
            product=self.product, build=self.build_name
        )
        assert build_metadata.version == self.version
        assert build_metadata.recipe.product == self.product

    def test_promote_to_drafts(self, setup_local_connectors):
        """Test promoting builds to drafts with revisions."""
        # Assert that no drafts exist initially
        draft_versions = drafts.get_versions(self.product)
        assert len(draft_versions) == 0, (
            f"No drafts should exist initially. Found {draft_versions}"
        )

        # Promote that build to a draft (no message)
        builds.promote_to_draft(
            product=self.product, build=self.build_name, draft_revision_summary=""
        )

        # Assert that the draft exists and the revision num is 1
        draft_versions = drafts.get_versions(self.product)
        assert len(draft_versions) == 1, "Should have one draft version"
        assert self.version in draft_versions[0], (
            f"Draft version should contain {self.version}"
        )

        revision_list = drafts.get_version_revisions(self.product, self.version)
        assert len(revision_list) == 1, "Should have one revision"
        assert "1" in revision_list[0], (
            f"First revision should be numbered 1. Found revisions {revision_list}"
        )

        # Promote again with a message
        builds.promote_to_draft(
            product=self.product,
            build=self.build_name,
            draft_revision_summary="fix-the-bug",
        )

        # Assert that the draft revision num is 2
        revision_list = drafts.get_version_revisions(self.product, self.version)
        assert len(revision_list) == 2, "Should have two revisions"

        # Check that we have revision 1 and 2
        revision_numbers = [int(rev.split("-")[0]) for rev in revision_list]
        assert 1 in revision_numbers, "Should have revision 1"
        assert 2 in revision_numbers, "Should have revision 2"

        # Check that revision 2 has the message
        revision_2 = [rev for rev in revision_list if rev.startswith("2-")][0]
        assert "fix-the-bug" in revision_2, "Revision 2 should contain the message"

    def test_publish_from_drafts(self, setup_local_connectors):
        """Test publishing drafts to published versions."""
        # Publish revision draft 1
        drafts.publish(product=self.product, version=self.version, draft_revision_num=1)

        # Assert that the published version exists
        published_versions = published.get_versions(self.product)
        assert len(published_versions) >= 1, (
            "Should have at least one published version"
        )
        assert self.version in published_versions, (
            f"Published version {self.version} should exist"
        )

        # Try: publish revision draft 2, without specifying patch flag, assert error
        with pytest.raises(ValueError, match="already exists.*patch wasn't selected"):
            drafts.publish(
                product=self.product,
                version=self.version,
                draft_revision_num=2,
                is_patch=False,
            )

        # Publish revision draft 2, specifying that it's a patch
        drafts.publish(
            product=self.product,
            version=self.version,
            draft_revision_num=2,
            is_patch=True,
        )

        # Assert that the published patched version exists
        published_versions_final = published.get_versions(self.product)
        assert len(published_versions_final) >= 2, (
            "Should have at least two published versions"
        )

        # Should have original version and patched version like 24v3.1
        version_strings = published_versions_final
        original_versions = [v for v in version_strings if v == self.version]
        patched_versions = [
            v
            for v in version_strings
            if v.startswith(f"{self.version}.") and v != self.version
        ]

        assert len(original_versions) >= 1, (
            f"Should have original {self.version} version"
        )
        assert len(patched_versions) >= 1, "Should have at least one patched version"
