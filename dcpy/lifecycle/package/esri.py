import re
from pathlib import Path

import typer
import yaml

import dcpy.models.product.dataset.metadata as models
from dcpy.utils.logging import logger


def _parse_raw_column(field_raw) -> models.Column:
    """
    Parse a column value from ESRI metadata pdfs
    example value: 'Field AssemDist,Alias ASSEMDIST,Data type String,Width 2,,,Field description\nState Assembly District Number'
    """
    parsed_field_info = {
        "description": "CHANGE_ME",
        "name": "CHANGE_ME",
        "display_name": "CHANGE_ME",
        "data_type": "CHANGE_ME",
    }

    field_token = "Field "
    alias_token = "Alias "
    type_token = "Data type"
    desc_token = "Field description\n"
    iters = 0
    while True:
        next_comma = field_raw.find(",")
        next_chunk = field_raw[0:next_comma]

        PROBABLY_AN_INFINITE_LOOP_COUNTER = 100
        if next_comma == -1 or iters > PROBABLY_AN_INFINITE_LOOP_COUNTER:
            if next_chunk.startswith(desc_token):
                # Description always comes last in the metadata
                parsed_field_info["description"] = field_raw[len(desc_token) :]
            break

        if next_chunk.startswith(field_token) and not next_chunk.startswith(
            field_token + "description"
        ):
            parsed_field_info["name"] = next_chunk[len(field_token) :]
        elif next_chunk.startswith(alias_token):
            parsed_field_info["display_name"] = next_chunk[len(alias_token) :]
        elif next_chunk.startswith(type_token):
            parsed_field_info["data_type"] = next_chunk[len(type_token) :]

        field_raw = field_raw[next_comma + 1 :]
        iters = iters + 1

    return models.Column(**parsed_field_info)  # type: ignore


app = typer.Typer()


@app.command("parse_pdf_text")
def parse_pdf_text(
    esri_pdf_path: Path,
    output_path: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Path to export the metadata to",
    ),
):
    text = open(esri_pdf_path, "r").read()
    text = (
        text.replace("\n_\n", "_")
        .replace("\n►\n*\n", ",")
        .replace("\n*\n", ",")
        .replace("Scale 0\n", ",")
        .replace("Precision 0,", ",")
    )
    fields_split_messy = re.split("\nHide Field .* ▲\n", text)[1:]
    fields = []
    for f in fields_split_messy:
        try:
            fields.append(_parse_raw_column(f).model_dump(exclude_none=True))
        except Exception as e:
            logger.error(str(e))

    output_path = output_path or Path("columns.yml")
    with open(output_path, "w") as outfile:
        yaml.dump(fields, outfile, sort_keys=False)
