import os
from pathlib import Path

import marimo
import uvicorn


MARIMO_PASSWORD = os.getenv("MARIMO_PASSWORD")
assert MARIMO_PASSWORD, (
    "Sorry, you're gonna need a password for this one. Set `MARIMO_PASSWORD` in your env."
)

NOTEBOOKS_DIR = os.getenv("MARMIMO_NOTEBOOK_DIR")
SERVER_PORT = int(os.getenv("MARIMO_PORT", "8080"))
SERVER_HOST = os.getenv("MARIMO_HOST", "0.0.0.0")


def create_server():
    app_dir = Path(__file__).parent

    notebooks_dir = (
        Path(NOTEBOOKS_DIR)
        if NOTEBOOKS_DIR
        else app_dir.parent.parent / "notebooks" / "marimo"
    )
    if not notebooks_dir.exists():
        raise Exception(f"Could not find notebook dir at: {notebooks_dir}")

    server = marimo.create_asgi_app(
        include_code=False, quiet=True, token=MARIMO_PASSWORD, session_ttl=3600
    )
    return (
        server.with_app(path="/", root=str(app_dir / "main.py"))
        .with_dynamic_directory(path="/notebooks/marimo", directory=str(notebooks_dir))
        .build()
    )


if __name__ == "__main__":
    app = create_server()
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)
