from pathlib import Path
from jinja2 import Template
import subprocess
from dcpy.models.product.dataset.metadata_v2 import Metadata

from . import RESOURCES_PATH

DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH = (
    RESOURCES_PATH / "data_dictionary_template.jinja"
)


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

    output_html_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_html_path, "w") as f:
        f.write(rendered_template)

    return output_html_path
