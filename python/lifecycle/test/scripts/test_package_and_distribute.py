from types import SimpleNamespace

from dcpy.lifecycle import models
from dcpy.lifecycle.scripts import package_and_distribute

DESTINATION = "developments.housing_database_project_level_files.socrata"


def test_failed_package_is_reported_not_raised(monkeypatch):
    org_md = SimpleNamespace(
        get_product_dataset_destinations=lambda _: SimpleNamespace(
            current_version=None
        ),
        product=lambda _: SimpleNamespace(
            dataset=lambda _: SimpleNamespace(
                attributes=SimpleNamespace(current_version="26q2.1")
            )
        ),
    )
    failed = models.PackageAssembleResult(
        product="developments",
        dataset="housing_database_project_level_files",
        version="26q2.1",
        source_id="bytes",
        success=False,
        result_summary="Error pulling package",
        result_details="404 Client Error",
    )
    monkeypatch.setattr(package_and_distribute.product_metadata, "load", lambda: org_md)
    monkeypatch.setattr(
        package_and_distribute,
        "get_destinations_by_product_dataset_and_type",
        lambda *_: [[DESTINATION]],
    )
    monkeypatch.setattr(
        package_and_distribute.package,
        "assemble_dataset_package",
        lambda **_: failed,
    )

    results = package_and_distribute.run(prod_ds_dest_filters={}, source_id="bytes")

    assert len(results) == 1
    result = results[0]
    assert not result.success
    assert result.destination_id == DESTINATION
    assert result.version == "26q2.1"
    assert result.result_details == "404 Client Error"
