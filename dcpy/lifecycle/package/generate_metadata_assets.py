from dcpy.models.product.dataset.metadata import Metadata, SocrataMetada
import argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import typer

app = typer.Typer()


@app.command()
def generate_metadata_assets(yaml_file_path: str, html_template_path: str, output_html_path: str) -> Path:
    html_output = to_html(yaml_file_path, html_template_path)
    output_path = Path(output_html_path)
    with open(output_path, "w") as f:
        f.write(html_output)
    return output_path


def to_html(yaml_file_path: str, html_template_path: str) -> str:
    env = Environment(loader=FileSystemLoader(Path(html_template_path).parent))
    template = env.get_template(Path(html_template_path).name)
    return template.render(metadata=get_metadata(yaml_file_path))


def get_metadata(yaml_file_path: str) -> SocrataMetada:
    metadata = Metadata.from_path(
        Path(yaml_file_path), template_vars={"var1": "value1"}
    )
    return metadata
