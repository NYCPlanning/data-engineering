import subprocess
from pathlib import Path

import css_inline
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader

from dcpy.models.product.dataset.metadata import Metadata
from dcpy.models.product.metadata import OrgMetadata
from dcpy.utils.logging import logger

from . import RESOURCES_PATH

DEFAULT_PDF_STYLESHEET_PATH = RESOURCES_PATH / "document_templates" / "paged_media.css"
DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH = (
    RESOURCES_PATH / "document_templates" / "data_dictionary.jinja"
)
DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH = (
    RESOURCES_PATH / "document_templates" / "document.css"
)


def _format_html(html: str) -> str:
    return BeautifulSoup(html, "html.parser").prettify(formatter="html")


def _render_html_template(template_path: Path, template_vars: dict) -> str:
    with open(template_path, "r") as f:
        template_text = f.read()

    compiled_template = Environment(
        loader=FileSystemLoader(template_path.parent)
    ).from_string(template_text)

    return _format_html(compiled_template.render(template_vars))


def _style_html(html: str, stylesheet_path: Path) -> str:
    with open(stylesheet_path, "r") as f:
        css = f.read()

    return _format_html(css_inline.inline_fragment(html, css))


def _style_html_document(html: str, stylesheet_path: Path) -> str:
    with open(stylesheet_path, "r") as f:
        css = f.read()

    inliner = css_inline.CSSInliner(extra_css=css)

    return _format_html(inliner.inline(html))


def generate_pdf_from_html(
    html_path: Path,
    output_path: Path,
    stylesheet_path: Path = DEFAULT_PDF_STYLESHEET_PATH,
) -> None:
    logger.info(f"Saving DCP PDF to {output_path}")
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


def generate_html_from_metadata(
    metadata: Metadata,
    output_path: Path,
    html_template_path: Path,
    stylesheet_path: Path,
) -> None:
    html = _render_html_template(html_template_path, {"metadata": metadata})
    styled_html = _style_html_document(html, stylesheet_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving DCP HTML to {output_path}")
    with open(output_path, "w") as f:
        f.write(styled_html)


def write_pdf(
    *,
    org_metadata: OrgMetadata,
    product: str,
    output_path: Path,
    dataset: str | None = None,
) -> None:
    if not dataset:
        dataset = product

    metadata = org_metadata.product(product).dataset(dataset)

    html_path = output_path.parent / "data_dictionary.html"
    generate_html_from_metadata(
        metadata,
        html_path,
        DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
        DEFAULT_DATA_DICTIONARY_STYLESHEET_PATH,
    )
    generate_pdf_from_html(
        html_path,
        output_path,
    )
