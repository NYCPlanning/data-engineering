import pandas as pd
import pytest
from pages.distribution import helpers


def _versions(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).set_index(["product", "dataset"])


def _row(product, dataset, destination_id="socrata", up_to_date=False, matches=True):
    return {
        "product": product,
        "dataset": dataset,
        "destination_id": destination_id,
        "up_to_date": up_to_date,
        "metadata_matches_bytes": matches,
    }


def test_up_to_date_rows_are_excluded():
    groups = helpers.outdated_groups(
        _versions(
            [
                _row("pluto", "pluto", up_to_date=True),
                _row("pluto", "mappluto", up_to_date=False),
            ]
        )
    )
    assert len(groups) == 1
    assert list(groups[0].ready["dataset"]) == ["mappluto"]


def test_datasets_of_a_product_batch_into_one_dispatch():
    groups = helpers.outdated_groups(
        _versions([_row("pluto", "pluto"), _row("pluto", "mappluto")])
    )
    assert len(groups) == 1
    assert sorted(groups[0].ready["dataset"]) == ["mappluto", "pluto"]


def test_destination_ids_do_not_batch_together():
    """The distribute CLI intersects --datasets with --destination-ids, so a batch spanning
    destination ids could redistribute an up-to-date pairing of the two."""
    groups = helpers.outdated_groups(
        _versions(
            [
                _row("dcm", "dcm", destination_id="socrata"),
                _row("dcm", "dcm_multi", destination_id="dcm_multilayer"),
            ]
        )
    )
    assert [(g.product, g.destination_id) for g in groups] == [
        ("dcm", "dcm_multilayer"),
        ("dcm", "socrata"),
    ]


def test_stale_metadata_version_blocks_a_dataset():
    groups = helpers.outdated_groups(
        _versions(
            [
                _row("pluto", "pluto", matches=True),
                _row("pluto", "mappluto", matches=False),
            ]
        )
    )
    assert list(groups[0].ready["dataset"]) == ["pluto"]
    assert list(groups[0].blocked["dataset"]) == ["mappluto"]


def test_group_with_nothing_ready_yields_no_datasets_to_dispatch():
    groups = helpers.outdated_groups(_versions([_row("pluto", "pluto", matches=False)]))
    assert groups[0].ready.empty
    assert list(groups[0].blocked["dataset"]) == ["pluto"]


def test_no_outdated_rows_yields_no_groups():
    assert (
        helpers.outdated_groups(_versions([_row("pluto", "pluto", up_to_date=True)]))
        == []
    )


@pytest.mark.parametrize(
    "bytes_version, metadata_version, expected",
    [
        ("25v4", "25v4", True),
        ("25v4", "25v3", False),
        ("20260401", "202604", True),
        ("", "25v4", False),
    ],
)
def test_metadata_matches_bytes_uses_fuzzy_comparison(
    bytes_version, metadata_version, expected
):
    from dcpy.lifecycle.scripts.version_compare import FuzzyVersion

    assert (
        FuzzyVersion(bytes_version).probably_equals(FuzzyVersion(metadata_version))
        is expected
    )
