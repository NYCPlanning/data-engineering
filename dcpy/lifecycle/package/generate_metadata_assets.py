from dcpy.models.product.dataset.metadata import Metadata, SocrataMetada
import argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import typer
import subprocess

app = typer.Typer()


@app.command("pdf_from_yml")
def _cli_wrapper_pdf_from_yml(
    yaml_file_path: str,
    html_template_path: str,
    output_html_path: str,
    output_pdf_path: str,
    pdf_metadata_path,
) -> None:
    generate_html_from_yaml(yaml_file_path, html_template_path, output_html_path)
    generate_pdf_from_html(output_html_path, pdf_metadata_path, output_pdf_path)
    return None


def generate_pdf_from_html(
    output_html_path: str, pdf_metadata_path: str, output_pdf_path: str
) -> Path:
    subprocess.run(
        [
            "pandoc",
            output_html_path,
            "-o",
            output_pdf_path,
            f"--metadata-file={pdf_metadata_path}",
        ],
        check=True,
    )
    return Path(output_pdf_path)


def generate_html_from_yaml(
    yaml_file_path: str, html_template_path: str, output_html_path: str
) -> Path:
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


if __name__ == "__main__":
    app()
