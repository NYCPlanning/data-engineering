from dcpy.models.product.dataset.metadata import Metadata 
import argparse
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

def save_html(yaml_file_path, html_template_path, output_html_path):
    html_output = to_html(yaml_file_path, html_template_path)
    output_path = Path(output_html_path)
    with open(output_path, 'w') as f:
        f.write(html_output)
    return output_path 

def to_html(yaml_file_path, html_template_path):
    env = Environment(loader=FileSystemLoader(Path(html_template_path).parent))
    template = env.get_template(Path(html_template_path).name)
    return template.render(metadata=get_metadata(yaml_file_path))

def get_metadata(yaml_file_path):
    metadata = Metadata.from_path(Path(yaml_file_path), template_vars={'var1': 'value1'})
    return metadata

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a YAML file to a HTML file."
    )
    parser.add_argument(
        "yaml_file_path", type=str, help="The path to the input YAML file"
    )
    parser.add_argument(
        "html_template_path", type=str, help="The path to the input HTML template file"
    )
    parser.add_argument(
        "output_html_path", type=str, help="The path to the output HTML file"
    )

    args = parser.parse_args()

    save_html(args.yaml_file_path, args.html_template_path, args.output_html_path)
