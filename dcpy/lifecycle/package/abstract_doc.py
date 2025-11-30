from pathlib import Path
from typing import Any

from dcpy.models.design import elements as de
from dcpy.models.product.artifacts import Artifact, ExcelTableComponentDefinition
from dcpy.models.product.dataset.metadata import Dataset
from dcpy.models.product.metadata import OrgMetadata
from dcpy.utils.logging import logger

# TODO; Extract these into a generic style that we can pass to the XLSX renderer
BLUE = "FF009DDC"
TITLE_FONT_SIZE = 18.0
MONOSPACED_FONT = "Consolas"


def _make_title_subtitle_cell(title: str, subtitle: str):
    return de.Cell(
        style=de.CellStyle(text_alignment_vertical="bottom"),
        value=[
            de.Cell(
                value=title + " - ",
                style=de.CellStyle(
                    font=de.Font(bold=True, size=TITLE_FONT_SIZE),
                ),
            ),
            de.Cell(
                value=subtitle,
                style=de.CellStyle(
                    font=de.Font(bold=True, rgb=BLUE, size=TITLE_FONT_SIZE),
                ),
            ),
        ],
    )


def _make_table_top(
    title: str,
    subtitle: str,
    description: str | None = None,
    maybe_image_path: Path | None = None,
    column_headers: list[str] = [],
    column_header_descriptions: list[str] = [],
):
    rows = []
    if maybe_image_path:
        rows.append(
            de.Row(  # maybe the image cell
                skip_default_styling=True,  # move
                height=100,  # move
                merge_cells=True,
                cells=[de.Cell(value=de.Image(path=maybe_image_path))],
            )
        )

    rows.append(
        de.Row(  # subtitle cell
            merge_cells=True,
            is_top_row=True,  # move
            height=30,  # move
            cells=[_make_title_subtitle_cell(title, subtitle)],
        )
    )
    if description:
        rows.append(
            de.Row(  # maybe the description cell
                merge_cells=True,
                height=50,
                cells=[
                    de.Cell(
                        value=description,
                        style=de.CellStyle(
                            text_alignment_vertical="center",
                            font=de.Font(italic=True, rgb=BLUE),
                        ),
                    ),
                ],
            )
        )
    if column_headers:
        rows.append(
            de.Row(
                cells=[
                    de.Cell(
                        style=de.CellStyle(
                            text_alignment_horizontal="center",
                            text_alignment_vertical="center",
                            font=de.Font(bold=True),
                        ),
                        value=col_name,
                    )
                    for col_name in column_headers
                ]
            )
        )
    if column_header_descriptions:
        rows.append(
            de.Row(
                cells=[
                    de.Cell(
                        style=de.CellStyle(
                            text_alignment_vertical="center",
                            font=de.Font(italic=True, rgb=BLUE),
                        ),
                        value=desc,
                    )
                    for desc in column_header_descriptions
                ]
            ),
        )

    return rows


def make_object_table(
    *,
    title: str,
    subtitle: str,
    table_rows: list[dict[str, str]],
    description: str | None = None,
    maybe_image_path: Path | None = None,
    column_widths: list[float] | None = None,
) -> de.Table:
    """Make a table for an object.
    Each row is an attribute of the object.
    There are only two columns
        - formatted key (summary + description)
        - value
    """
    rows = _make_table_top(
        title=title,
        subtitle=subtitle,
        description=description,
        maybe_image_path=maybe_image_path,
    )

    for r in table_rows:
        value = r.get("value")
        if value is None:
            logger.warning(f"Metadata field is empty for {r}")
        elif type(value) is list and type(value[0]) is str:
            value = ", ".join(value)

        rows.append(
            de.Row(
                cells=[
                    de.Cell(  # <b>field title \n summary<b> for each field
                        value=[
                            de.Cell(
                                value="\n" + r["summary"] + "\n",  # This isn't great
                                style=de.CellStyle(
                                    font=de.Font(bold=True, size=11),
                                ),
                            ),
                            de.Cell(
                                value=(r["description"]),
                                style=de.CellStyle(
                                    font=de.Font(size=9, italic=True),
                                ),
                            ),
                        ],
                    ),
                    de.Cell(  # Value
                        value=value,
                        style=de.CellStyle(font=de.Font(italic=True)),
                    ),
                ]
            )
        )

    return de.Table(
        title=title,
        subtitle=subtitle,
        description=description,
        rows=rows,
        column_widths=column_widths or [50, 80],
    )


def make_list_table(
    title: str,
    subtitle: str,
    table_rows: list[list[Any]],
    column_headers: list[str] = [],
    column_ids: list[str] = [],
    column_descriptions: list[str] = [],
    description: str | None = None,
    maybe_image_path: Path | None = None,
    column_widths: list[float] | None = None,
    style_overrides: dict[str, de.CellStyle] = {},
):
    """Make a table for a list of objects. e.g. Revisions or columns
    There is one row per column, and columns can be specified
    """
    rows = _make_table_top(
        title=title,
        subtitle=subtitle,
        description=description,
        maybe_image_path=maybe_image_path,
        column_headers=column_headers,
        column_header_descriptions=column_descriptions,
    )

    for tr in table_rows:
        cells = []
        for val, col_id in zip(tr, column_ids):
            style = style_overrides.get(col_id, de.CellStyle())
            style.text_alignment_vertical = style.text_alignment_vertical or "top"
            cells.append(de.Cell(value=val, style=style))
        rows.append(de.Row(cells=cells))

    return de.Table(
        title=title,
        subtitle=subtitle,
        description=description,
        rows=rows,
        column_widths=column_widths or [50, 80],
    )


# TODO: move to pydantic models
def get_field_metadata(data_source: str, org_metadata: OrgMetadata):
    match data_source:
        case "dataset.columns":
            dictionary_section = org_metadata.data_dictionary.dataset["columns"]
        case "dataset.revisions":
            dictionary_section = org_metadata.data_dictionary.dataset["revisions"]
        case "dataset.attributes":
            dictionary_section = org_metadata.data_dictionary.dataset["attributes"]
        case _:
            raise Exception(f"Unknown data_source: {data_source}")
    return dictionary_section


# TODO: move to pydantic models
def get_data_source(
    data_source: str, dataset: Dataset, columns: list[str] = []
) -> list[list[Any]]:
    model_dict = dataset.model_dump()
    match data_source:
        case "dataset.columns":
            source = [c.all_fields_repr() for c in dataset.columns]
            if columns:
                return [[r.get(c_name) for c_name in columns] for r in source]
            else:
                return [[k, v] for k, v in source]
        case "dataset.revisions":
            source = model_dict.get("revisions", [])
            # convert a list[dict] -> list[str], ordered as spec'd in `columns`
            columns = columns or ["date", "summary", "notes"]
            return [[r.get(c_name) for c_name in columns] for r in source]
        case "dataset.attributes":
            return [[k, v] for k, v in dataset.attributes.model_dump().items()]
        case _:
            raise Exception(f"Unknown data_source: {data_source}")


def construct_component(
    component_def: ExcelTableComponentDefinition,
    dataset: Dataset,
    org_metadata: OrgMetadata,
) -> de.Table:
    full_image_path = (
        org_metadata.get_full_resource_path(component_def.image_path)
        if component_def.image_path
        else None
    )

    dictionary_section = get_field_metadata(
        data_source=component_def.data_source or "", org_metadata=org_metadata
    )

    match component_def.type:
        case "object_table":
            # If it's an object_table, then a table row represents a field on a model.
            table_rows = []
            data_source = get_data_source(
                data_source=component_def.data_source or "",
                dataset=dataset,
            )
            data_source_dict = dict(data_source)

            for field_name in component_def.rows or []:
                data_dict_field = dictionary_section[field_name]
                description_paragraphs = [data_dict_field.extra_description or ""]
                if component_def.extra_field_description_field:
                    description_paragraphs.append(
                        data_dict_field.custom.get(
                            component_def.extra_field_description_field, ""
                        )
                    )
                table_rows.append(
                    {
                        "field_name": field_name,
                        "title": field_name,
                        "summary": data_dict_field.summary,
                        "description": "\n".join(description_paragraphs),
                        "value": data_source_dict.get(field_name),
                    }
                )

            return make_object_table(
                title=component_def.title,
                subtitle=component_def.subtitle,
                description=component_def.description,
                table_rows=table_rows,
                maybe_image_path=full_image_path,
                column_widths=component_def.column_widths,
            )
        case "list_table":
            # For list tables, unlike object_tables, `field_metadata_rows` will inform the columns, not the rows
            data_source = get_data_source(
                data_source=component_def.data_source or "",
                columns=component_def.columns or [],
                dataset=dataset,
            )

            assert component_def.columns, (
                "Columns must be specified for list_table type."
            )
            column_headers = []
            column_descriptions = []
            style_overrides = {}
            for f in component_def.columns:
                field = dictionary_section[f]
                column_headers.append(field.summary)
                column_descriptions.append(field.extra_description)
                if (
                    component_def.data_source == "dataset.columns"
                    and "values" in component_def.columns
                ):  # The abstraction has leaked!
                    style_overrides["values"] = de.CellStyle(
                        font=de.Font(name=MONOSPACED_FONT)
                    )

            return make_list_table(
                title=component_def.title,
                subtitle=component_def.subtitle,
                description=component_def.description,
                column_ids=component_def.columns,
                column_headers=column_headers,
                column_descriptions=column_descriptions
                if component_def.include_column_description_row
                else [],
                table_rows=data_source,
                maybe_image_path=full_image_path,
                column_widths=component_def.column_widths,
                style_overrides=style_overrides,
            )
        case _:
            raise Exception(f"Component type {component_def.type} not implemented.")


def generate_abstract_artifact(
    product: str, dataset: str, artifact: Artifact, org_metadata: OrgMetadata
):
    ds = org_metadata.product(product).dataset(dataset).dataset

    return [
        construct_component(component_def=c, dataset=ds, org_metadata=org_metadata)
        for c in artifact.components
    ]
