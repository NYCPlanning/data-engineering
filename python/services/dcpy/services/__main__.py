"""`python -m dcpy.services ...`: its own entry point rather than a group under `python -m dcpy`,
whose root CLI lives in dcpy-lifecycle, which mustn't depend on dcpy-services."""

from dcpy.services._cli import app

if __name__ == "__main__":
    app()
