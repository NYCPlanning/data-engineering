#!/usr/bin/env python3
"""
Marimo server for data engineering operational notebooks.

This server hosts multiple read-only marimo notebooks for operational tasks:
- Build runner: Execute and monitor data pipeline builds
- Data distribution: View and manage data distribution tasks
- Version checker: Check available versions of data products
"""

import marimo
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse


def create_server():
    """Create the marimo server with all operational notebooks."""

    # Create the base marimo server
    server = marimo.create_asgi_app(
        include_code=True,  # Show code in read-only mode
        quiet=False,
    )

    # Path to notebooks directory
    notebooks_dir = Path(__file__).parent / "notebooks"

    # Manually add specific operational notebooks
    operational_notebooks = {
        "build": "build_runner.py",
        "distribute": "data_distribution.py",
        "distribution": "distribution.py",
        "versions": "version_checker.py",
        "connectors": "connector_management.py",
    }

    # Add each operational notebook
    for route_name, filename in operational_notebooks.items():
        notebook_path = notebooks_dir / filename
        if notebook_path.exists():
            server = server.with_app(path=f"/{route_name}", root=notebook_path)
            print(f"Added notebook: /{route_name} -> {filename}")
        else:
            print(f"Warning: Notebook not found: {filename}")

    # Also dynamically add any other notebooks in the directory
    for notebook_file in notebooks_dir.glob("*.py"):
        route_name = notebook_file.stem
        # Skip if already added above
        if route_name not in [
            nb.replace("_", "-") for nb in operational_notebooks.keys()
        ]:
            server = server.with_app(path=f"/{route_name}", root=notebook_file)
            print(f"Added notebook: /{route_name} -> {notebook_file.name}")

    return server


def create_index_page():
    """Create a simple index page listing available notebooks."""
    notebooks_dir = Path(__file__).parent / "notebooks"

    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Engineering Operational Notebooks</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            h1 { color: #2c3e50; }
            .notebook-list { list-style-type: none; padding: 0; }
            .notebook-item {
                margin: 10px 0;
                padding: 15px;
                background: #f8f9fa;
                border-radius: 5px;
                border-left: 4px solid #007bff;
            }
            .notebook-link {
                text-decoration: none;
                color: #007bff;
                font-weight: bold;
                font-size: 1.1em;
            }
            .notebook-link:hover { text-decoration: underline; }
            .description { color: #6c757d; margin-top: 5px; }
        </style>
    </head>
    <body>
        <h1>🛠️ Data Engineering Operational Notebooks</h1>
        <p>Select a notebook to run operational tasks:</p>

        <ul class="notebook-list">
            <li class="notebook-item">
                <a href="/build" class="notebook-link">Build Runner</a>
                <div class="description">Execute and monitor data pipeline builds</div>
            </li>
            <li class="notebook-item">
                <a href="/distribute" class="notebook-link">Data Distribution</a>
                <div class="description">View and manage data distribution tasks</div>
            </li>
            <li class="notebook-item">
                <a href="/versions" class="notebook-link">Version Checker</a>
                <div class="description">Check available versions of data products</div>
            </li>
            <li class="notebook-item">
                <a href="/connectors" class="notebook-link">Connector Management</a>
                <div class="description">Explore registered connectors and manage dataset versions</div>
            </li>
        </ul>

        <hr style="margin: 30px 0;">
        <h2>Available Notebooks</h2>
        <ul class="notebook-list">
    """

    # Add any additional notebooks found
    for notebook_file in notebooks_dir.glob("*.py"):
        route_name = notebook_file.stem
        html += f"""
            <li class="notebook-item">
                <a href="/{route_name}" class="notebook-link">{route_name.replace("_", " ").title()}</a>
                <div class="description">Notebook: {notebook_file.name}</div>
            </li>
        """

    html += """
        </ul>

        <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #6c757d;">
            <p>Powered by <a href="https://marimo.io">marimo</a> | NYC DCP Data Engineering</p>
        </footer>
    </body>
    </html>
    """

    return html


# Create FastAPI wrapper to add index page
app = FastAPI(title="Data Engineering Marimo Server")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve index page with links to notebooks."""
    return create_index_page()


# Create and mount the marimo server
marimo_server = create_server()
app.mount("/", marimo_server.build())


if __name__ == "__main__":
    import uvicorn

    print("🚀 Starting Data Engineering Marimo Server...")
    print("📖 Available at: http://localhost:8080")
    print("📝 Notebooks directory: ./notebooks/")

    uvicorn.run("server:app", host="0.0.0.0", port=8888, reload=True, log_level="info")
