from dcpy.models.product.dataset.metadata import (
    DatasetAttributes,
    DatasetColumn,
    DatasetOverrides,
    File,
    FileAndOverrides,
    GdbLayerOverrides,
    Metadata,
)


def _make_metadata(layers: list[GdbLayerOverrides] | None = None) -> Metadata:
    return Metadata(
        id="test",
        attributes=DatasetAttributes(display_name="Test", each_row_is_a="row"),
        columns=[DatasetColumn(id="borough", name="Borough", data_type="text")],
        files=[
            FileAndOverrides(
                file=File(
                    id="my_gdb",
                    filename="my.gdb",
                    type="geodatabase",
                    layers=layers,
                ),
                dataset_overrides=DatasetOverrides(
                    overridden_columns=[DatasetColumn(id="borough", data_type="String")]
                ),
            )
        ],
    )


def test_calculate_layer_dataset_metadata_three_level_merge():
    """base → file-level → layer-level: all three levels are applied."""
    md = _make_metadata(
        layers=[
            GdbLayerOverrides(
                layer="my_layer",
                overridden_columns=[DatasetColumn(id="borough", description="Layer desc")],
            )
        ]
    )
    result = md.calculate_layer_dataset_metadata(file_id="my_gdb", layer="my_layer")
    col = result.columns[0]
    assert col.data_type == "String"  # from file-level override
    assert col.description == "Layer desc"  # from layer-level override


def test_calculate_layer_dataset_metadata_unmatched_layer_returns_file_level():
    """Layer name not in file.layers → layer-level overrides not applied."""
    md = _make_metadata(
        layers=[
            GdbLayerOverrides(
                layer="my_layer",
                overridden_columns=[DatasetColumn(id="borough", description="Layer desc")],
            )
        ]
    )
    result = md.calculate_layer_dataset_metadata(file_id="my_gdb", layer="other_layer")
    col = result.columns[0]
    assert col.data_type == "String"  # file-level override still applied
    assert col.description is None  # layer-level description not applied


def test_calculate_layer_dataset_metadata_no_layers_declared_returns_file_level():
    """file.layers is None → file-level result returned unchanged."""
    md = _make_metadata(layers=None)
    result = md.calculate_layer_dataset_metadata(file_id="my_gdb", layer="any_layer")
    assert result.columns[0].data_type == "String"
