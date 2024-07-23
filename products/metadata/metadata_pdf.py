import yaml
import argparse
from markdown_pdf import MarkdownPdf
from markdown_pdf import Section


def yaml_to_pdf(yaml_file_path: str, pdf_file_path: str) -> str:
    markdown_file_path = yaml_to_markdown(yaml_file_path, pdf_file_path)
    pdf = MarkdownPdf(toc_level=2)
    pdf.add_section(Section(open(markdown_file_path, encoding="utf-8").read()))
    pdf.save(pdf_file_path)
    return pdf_file_path


def yaml_to_markdown(yaml_file_path: str, markdown_file_path: str) -> str:
    data = read_yaml(yaml_file_path)
    markdown_str = dict_to_markdown(data)

    with open(markdown_file_path, "w") as markdown_file:
        markdown_file.write(markdown_str)
    return markdown_file_path


def read_yaml(file_path: str) -> dict:
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
    return data


def dict_to_markdown(d: dict, indent=0) -> str:
    md_str = ""
    indent_str = "  " * indent
    for key, value in d.items():
        if isinstance(value, dict):
            md_str += f"{indent_str}- **{key}**:\n{dict_to_markdown(value, indent + 1)}"
        elif isinstance(value, list):
            md_str += f"{indent_str}- **{key}**:\n{list_to_markdown(value, indent + 1)}"
        else:
            md_str += f"{indent_str}- **{key}**: {value}\n"
    return md_str


def list_to_markdown(lst: list, indent=0) -> str:
    md_str = ""
    indent_str = "  " * indent
    for item in lst:
        if isinstance(item, dict):
            md_str += f"{dict_to_markdown(item, indent)}"
        elif isinstance(item, list):
            md_str += f"{list_to_markdown(item, indent)}"
        else:
            md_str += f"{indent_str}- {item}\n"
    return md_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a YAML file to a Markdown file."
    )
    parser.add_argument(
        "yaml_file_path", type=str, help="The path to the input YAML file"
    )
    parser.add_argument(
        "pdf_file_path", type=str, help="The path to the output PDF file"
    )

    args = parser.parse_args()

    yaml_to_pdf(args.yaml_file_path, args.pdf_file_path)
