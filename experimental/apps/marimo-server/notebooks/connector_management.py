import marimo as mo
import pandas as pd
from typing import Any, Dict, List
import random
from datetime import datetime, timedelta

app = mo.App(width="medium")


@app.cell
def __():
    """Import and setup the connector registry"""
    try:
        from dcpy.lifecycle.connector_registry import connectors
        from dcpy.connectors.registry import VersionedConnector, VersionSearch

        mo.md("""
        # 🔌 Connector Management Dashboard
        
        This dashboard provides an interface to explore registered connectors and manage their versions.
        """)
    except ImportError as e:
        mo.md(f"""
        # ⚠️ Import Error
        
        Could not import dcpy connectors: {e}
        
        Make sure dcpy is installed as an editable package:
        ```bash
        pip install -e .
        ```
        """)
    return


@app.cell
def __(connectors):
    """Get list of registered connectors"""
    registered_connectors = connectors.list_registered()

    mo.md(f"""
    ## Available Connectors ({len(registered_connectors)})
    
    The following connectors are currently registered in the system:
    """)
    return (registered_connectors,)


@app.cell
def __(mo, registered_connectors):
    """Create dropdown for connector selection"""
    if registered_connectors:
        connector_dropdown = mo.ui.dropdown(
            options=registered_connectors,
            value=registered_connectors[0] if registered_connectors else None,
            label="Select a connector:",
        )
    else:
        connector_dropdown = mo.ui.dropdown(
            options=[], value=None, label="No connectors available"
        )

    connector_dropdown
    return (connector_dropdown,)


@app.cell
def __(mo, connector_dropdown, connectors):
    """Display connector info and setup version interface"""
    if not connector_dropdown.value:
        mo.md("👆 Select a connector to view its details")
    else:
        selected_connector_name = connector_dropdown.value

        try:
            # Get the connector instance
            connector_instance = connectors[selected_connector_name]
            connector_type = type(connector_instance).__name__

            # Check if it supports versioning
            supports_versioning = hasattr(connector_instance, "list_versions")

            mo.md(f"""
            ## 📋 Connector Details: `{selected_connector_name}`
            
            - **Type**: {connector_type}
            - **Supports Versioning**: {"✅ Yes" if supports_versioning else "❌ No"}
            - **Module**: {type(connector_instance).__module__}
            """)

        except Exception as e:
            mo.md(f"""
            ## ❌ Error loading connector: `{selected_connector_name}`
            
            ```
            {str(e)}
            ```
            """)
    return


@app.cell
def __(mo, connector_dropdown, connectors):
    """Version management interface"""
    if not connector_dropdown.value:
        mo.stop()

    selected_connector_name = connector_dropdown.value

    try:
        connector_instance = connectors[selected_connector_name]
        supports_versioning = hasattr(connector_instance, "list_versions")

        if supports_versioning:
            # Create input for dataset key
            dataset_key_input = mo.ui.text(
                placeholder="Enter dataset key (e.g., 'abc123' for Socrata)",
                label="Dataset Key:",
                full_width=True,
            )

            # Refresh button
            refresh_button = mo.ui.button(label="🔄 Refresh Versions", kind="success")

            mo.md(f"""
            ## 📊 Version Management
            
            This connector supports version operations. Enter a dataset key to explore versions:
            """)

            mo.vstack([dataset_key_input, refresh_button])
        else:
            mo.md(f"""
            ## ℹ️ Version Management Not Available
            
            The `{selected_connector_name}` connector does not support version operations.
            """)
    except Exception as e:
        mo.md(f"Error setting up version interface: {str(e)}")
    return


@app.cell
def __(mo, connector_dropdown, connectors, dataset_key_input, refresh_button):
    """Display versions table"""
    if not connector_dropdown.value:
        mo.stop()

    if (
        not hasattr(locals().get("dataset_key_input"), "value")
        or not dataset_key_input.value
    ):
        mo.md("👆 Enter a dataset key above to see versions")
        mo.stop()

    selected_connector_name = connector_dropdown.value
    dataset_key = dataset_key_input.value.strip()

    if not dataset_key:
        mo.stop()

    try:
        connector_instance = connectors[selected_connector_name]

        if hasattr(connector_instance, "list_versions"):
            # Mock version data for demonstration
            # In reality, we'd call: versions = connector_instance.list_versions(dataset_key)
            mock_versions = [f"v1.{i}.{random.randint(0, 9)}" for i in range(10, 0, -1)]

            # Create mock metadata for each version
            version_data = []
            for i, version in enumerate(mock_versions):
                created_date = datetime.now() - timedelta(
                    days=i * 7 + random.randint(0, 6)
                )
                size_mb = round(random.uniform(0.1, 150.0), 2)
                status = random.choice(
                    ["Active", "Active", "Active", "Deprecated", "Draft"]
                )

                version_data.append(
                    {
                        "Version": version,
                        "Created": created_date.strftime("%Y-%m-%d %H:%M"),
                        "Size (MB)": size_mb,
                        "Status": status,
                        "Records": random.randint(100, 50000),
                    }
                )

            df = pd.DataFrame(version_data)

            # Style the dataframe
            styled_df = df.style.apply(
                lambda x: [
                    "background-color: #e8f5e8"
                    if v == "Active"
                    else "background-color: #fff2e8"
                    if v == "Draft"
                    else "background-color: #f5e8e8"
                    if v == "Deprecated"
                    else ""
                    for v in x
                ],
                subset=["Status"],
            )

            mo.md(f"""
            ## 📈 Versions for Dataset: `{dataset_key}`
            
            Found **{len(mock_versions)}** versions for connector `{selected_connector_name}`:
            
            > ⚠️ **Note**: This is mock data for demonstration. In production, this would call:
            > ```python
            > connector_instance.list_versions("{dataset_key}")
            > ```
            """)

            mo.ui.table(df, selection=None)

        else:
            mo.md("This connector does not support version listing.")

    except Exception as e:
        mo.md(f"""
        ## ❌ Error fetching versions
        
        ```
        {str(e)}
        ```
        
        **Possible reasons:**
        - Dataset key not found
        - Network connectivity issues  
        - Connector configuration problems
        - Authentication required
        """)
    return


@app.cell
def __(mo, connector_dropdown, connectors):
    """Connector capabilities summary"""
    if not connector_dropdown.value:
        mo.stop()

    selected_connector_name = connector_dropdown.value

    try:
        connector_instance = connectors[selected_connector_name]

        # Check capabilities
        capabilities = {
            "Pull Data": hasattr(connector_instance, "pull"),
            "Push Data": hasattr(connector_instance, "push"),
            "List Versions": hasattr(connector_instance, "list_versions"),
            "Get Latest Version": hasattr(connector_instance, "get_latest_version"),
            "Version Exists Check": hasattr(connector_instance, "version_exists"),
        }

        capability_rows = []
        for capability, supported in capabilities.items():
            status = "✅ Supported" if supported else "❌ Not Available"
            capability_rows.append({"Capability": capability, "Status": status})

        capabilities_df = pd.DataFrame(capability_rows)

        mo.md("## 🛠️ Connector Capabilities")
        mo.ui.table(capabilities_df, selection=None)

    except Exception as e:
        mo.md(f"Error checking capabilities: {str(e)}")
    return


if __name__ == "__main__":
    app.run()
