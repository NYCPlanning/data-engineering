from pathlib import Path
from jinja2 import Template
import typer
import subprocess
from dcpy.models.product.dataset.metadata import Metadata

DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH = (
    Path(__file__).parent / "resources" / "data_dictionary_template.jinja"
)

app = typer.Typer()


@app.command("pdf_from_yml")
def _cli_wrapper_pdf_from_yml(
    yaml_file_path: str,
    html_template_path: str,
    output_html_path: str,
    output_pdf_path: str,
) -> None:
    generate_html_from_yaml(
        Path(yaml_file_path),
        Path(html_template_path),
        Path(output_html_path),
    )
    generate_pdf_from_html(Path(output_html_path), Path(output_pdf_path))
    return None


def generate_pdf_from_html(output_html_path: Path, output_pdf_path: Path) -> Path:
    subprocess.run(
        [
            "pandoc",
            output_html_path,
            "-o",
            output_pdf_path,
            "--pdf-engine=weasyprint",
        ],
        check=True,
    )
    return Path(output_pdf_path)


def generate_html_from_yaml(
    yaml_file_path: Path,
    output_html_path: Path,
    html_template_path: Path,
) -> Path:
    metadata = Metadata.from_path(yaml_file_path, template_vars={"var1": "value1"})

    with open(html_template_path, "r") as f:
        template_text = f.read()
    rendered_template = Template(template_text).render({"metadata": metadata})

    if not output_html_path.parent.exists():
        output_html_path.parent.mkdir(parents=True)
    with open(output_html_path, "w") as f:
        f.write(rendered_template)

    return output_html_path


if __name__ == "__main__":
    app()
