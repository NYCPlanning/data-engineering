"""
Which of a build's exports have a production counterpart to diff against.

Both validation paths need this rule: validate_outputs.sh compares a build's local
output, run_validation.py compares one already published. Keeping the rule here stops
the two from disagreeing about what gets compared.
"""

from pathlib import Path

from dcpy.lifecycle.builds import plan

COMPARE_FILE = "compare_file"


def compares_to_prod(export) -> bool:
    """Whether an export is diffed against the production file of the same name."""
    return (export.custom or {}).get(COMPARE_FILE, True)


def excluded_filenames(recipe_path: Path = Path("recipe.yml")) -> set[str]:
    """Filenames of exports with no production counterpart to compare against."""
    recipe = plan.recipe_from_yaml(recipe_path)
    assert recipe.exports
    return {
        export.filename
        for export in recipe.exports.datasets
        if export.filename and not compares_to_prod(export)
    }
