from pathlib import Path
from jinja2 import Template
import subprocess
from dcpy.models.product.dataset.metadata_v2 import Metadata

from . import RESOURCES_PATH

DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH = (
    RESOURCES_PATH / "data_dictionary_template.jinja"
)
DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH = RESOURCES_PATH / "data_dictionary.css"


def generate_pdf_from_html(
    html_path: Path,
    output_path: Path,
    stylesheet_path: Path,
) -> Path:
    subprocess.run(
        [
            "weasyprint",
            html_path,
            output_path,
            "-s",
            stylesheet_path,
        ],
        check=True,
    )
    return output_path


def generate_html_from_yaml(
    metadata_path: Path,
    output_path: Path,
    html_template_path: Path,
) -> Path:
    metadata = Metadata.from_path(metadata_path, template_vars={"var1": "value1"})

    with open(html_template_path, "r") as f:
        template_text = f.read()
    rendered_template = Template(template_text).render({"metadata": metadata})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(rendered_template)

    return output_path
