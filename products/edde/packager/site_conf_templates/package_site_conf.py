"""
Package EDDE site configuration files by templatizing JSON templates.

This module renders Jinja2 templates from site_conf_templates/ using variables
from recipe.lock.yml and outputs them to the build output package directory.
"""

import json
import sys
from pathlib import Path

# Add products/edde to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config import PRODUCT_PATH, get_build_output_dir  # noqa: E402
from jinja2 import Environment, FileSystemLoader

from dcpy.lifecycle.builds import get_recipe_lock  # noqa: E402
from dcpy.utils.logging import logger  # noqa: E402


def get_template_variables() -> dict:
    """
    Extract template variables from recipe.lock.yml.

    Returns:
        Dictionary containing all template variables including:
        - All vars (BUILD_ENV_EDDE_ACS_CURRENT_YEAR_BAND, etc.)
        - All custom variables (dhs_shelter, health_mortality, etc.)
    """
    recipe_lock = get_recipe_lock(PRODUCT_PATH)

    # Start with all vars
    template_vars = dict(recipe_lock.vars) if recipe_lock.vars else {}

    # Add all custom variables
    if recipe_lock.custom:
        template_vars.update(recipe_lock.custom)

    logger.info(f"Loaded template variables: {list(template_vars.keys())}")
    return template_vars


def expand_year_range(year_code: str) -> str:
    """
    Convert abbreviated year code to full year range.

    Args:
        year_code: Year code like "0812", "1923", or "2024"

    Returns:
        Full year range like "2008-2012", "2019-2023", or "2024"

    Examples:
        "0812" -> "2008-2012"
        "1923" -> "2019-2023"
        "2024" -> "2024"
    """
    year_code = str(year_code)

    # Handle 4-digit range codes (e.g., "0812" or "1923")
    if len(year_code) == 4:
        first_two = year_code[:2]
        last_two = year_code[2:]

        # Convert to full years
        # Assume 20XX for years >= 00, otherwise 19XX (though we don't expect this)
        first_year = f"20{first_two}"
        last_year = f"20{last_two}"

        return f"{first_year}-{last_year}"

    # For other formats (like "2024"), return as-is
    return year_code


def render_template(template_path: Path, variables: dict) -> str:
    """
    Render a single JSON template file with Jinja2.

    Args:
        template_path: Path to the template file
        variables: Dictionary of template variables

    Returns:
        Rendered template as a string
    """
    # Set up Jinja2 environment
    env = Environment(
        loader=FileSystemLoader(template_path.parent),
        # Preserve JSON formatting
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )

    # Add custom filter for expanding year ranges
    env.filters["expand_year_range"] = expand_year_range

    template = env.get_template(template_path.name)
    rendered = template.render(**variables)

    return rendered


def validate_json(content: str, file_path: Path) -> None:
    """
    Validate that the rendered content is valid JSON.

    Args:
        content: The rendered JSON string
        file_path: Path to the file (for error messages)

    Raises:
        json.JSONDecodeError: If the content is not valid JSON
    """
    try:
        json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Rendered template {file_path.name} is not valid JSON: {e}"
        ) from e


def package_site_conf() -> None:
    """
    Package all site configuration templates into the build output directory.

    This function:
    1. Loads template variables from recipe.lock.yml
    2. Renders each JSON template in site_conf_templates/
    3. Validates the rendered JSON
    4. Writes output to {build_output_dir}/package/site_conf/
    """
    logger.info("Starting site configuration packaging")

    # Get template variables
    variables = get_template_variables()

    # Get paths
    templates_dir = PRODUCT_PATH / "packager" / "site_conf_templates" / "templates"
    build_output_dir = get_build_output_dir()
    output_dir = build_output_dir / "package" / "site_conf"

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Find all JSON template files
    template_files = list(templates_dir.glob("*.json"))

    if not template_files:
        logger.warning(f"No template files found in {templates_dir}")
        return

    logger.info(f"Found {len(template_files)} template files to process")

    # Process each template
    for template_path in template_files:
        logger.info(f"Processing template: {template_path.name}")

        # Render template
        rendered = render_template(template_path, variables)

        # Validate JSON
        validate_json(rendered, template_path)

        # Write to output
        output_path = output_dir / template_path.name
        output_path.write_text(rendered)
        logger.info(f"  ✓ Written to: {output_path}")

    logger.info(f"✓ Successfully packaged {len(template_files)} configuration files")


if __name__ == "__main__":
    package_site_conf()
