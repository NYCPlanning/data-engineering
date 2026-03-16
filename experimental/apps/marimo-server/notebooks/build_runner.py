import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo

    mo.md(
        r"""
        # 🏗️ Data Pipeline Build Runnersss
        
        This notebook provides an interface to execute and monitor data pipeline builds
        for NYC DCP data products.
        """
    )
    return (mo,)


@app.cell
def __(mo):
    import subprocess
    import sys
    from pathlib import Path
    import json
    from datetime import datetime

    # Add dcpy to path
    repo_root = Path(__file__).parents[3]  # Go up to data-engineering root
    products_dir = repo_root / "products"

    mo.md(f"📁 **Products Directory**: `{products_dir}`")
    return Path, datetime, json, products_dir, repo_root, subprocess, sys


@app.cell
def __(mo, products_dir):
    # Discover available products
    available_products = []
    if products_dir.exists():
        for product_dir in products_dir.iterdir():
            if product_dir.is_dir() and (product_dir / "dbt_project.yml").exists():
                available_products.append(product_dir.name)

    if available_products:
        product_selector = mo.ui.dropdown(
            options=sorted(available_products), label="Select Product to Build:"
        )

        mo.md(f"""
        ## Available Products ({len(available_products)})
        
        {product_selector}
        """)
    else:
        product_selector = None
        mo.md("❌ No dbt products found in products directory")
    return available_products, product_selector


@app.cell
def __(mo, product_selector):
    # Build configuration options
    if product_selector and product_selector.value:
        # Build mode selector
        build_mode = mo.ui.dropdown(
            options=[
                ("Full Build", "run"),
                ("Test Only", "test"),
                ("Compile Only", "compile"),
                ("Parse Only", "parse"),
            ],
            value="run",
            label="Build Mode:",
        )

        # Model selector
        model_selector = mo.ui.text(
            placeholder="e.g., +model_name, tag:staging, --exclude test_type:unit",
            label="Model Selection (optional):",
            full_width=True,
        )

        # Environment selector
        profile_selector = mo.ui.dropdown(
            options=[
                ("Local Development", "local"),
                ("Development", "development"),
                ("Staging", "staging"),
                ("Production", "production"),
            ],
            value="local",
            label="Target Environment:",
        )

        mo.md(f"""
        ### Build Configuration for {product_selector.value}
        
        {build_mode}
        {model_selector}
        {profile_selector}
        """)
    else:
        build_mode = model_selector = profile_selector = None
        mo.md("")
    return build_mode, model_selector, profile_selector


@app.cell
def __(mo):
    # Build execution status
    build_status = {
        "running": False,
        "last_run": None,
        "output": "",
        "return_code": None,
    }

    mo.md("## Build Execution")
    return (build_status,)


@app.cell
def __(
    build_mode,
    build_status,
    datetime,
    mo,
    model_selector,
    product_selector,
    products_dir,
    profile_selector,
    subprocess,
):
    # Build execution function
    def run_dbt_command():
        """Execute the dbt command based on selected options."""
        if not product_selector or not product_selector.value:
            return "❌ No product selected"

        product_path = products_dir / product_selector.value
        if not product_path.exists():
            return f"❌ Product directory not found: {product_path}"

        # Build the dbt command
        cmd = ["dbt", build_mode.value if build_mode else "run"]

        # Add model selection if specified
        if model_selector and model_selector.value.strip():
            cmd.extend(["--select", model_selector.value.strip()])

        # Add profile directory (assumes profiles.yml in product dir)
        cmd.extend(["--profiles-dir", str(product_path)])

        # Add target environment
        if profile_selector and profile_selector.value != "local":
            cmd.extend(["--target", profile_selector.value])

        build_status["running"] = True
        build_status["last_run"] = datetime.now()

        try:
            # Execute the command
            result = subprocess.run(
                cmd,
                cwd=product_path,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            build_status["output"] = (
                f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
            )
            build_status["return_code"] = result.returncode

            if result.returncode == 0:
                status_msg = f"✅ Build completed successfully!"
            else:
                status_msg = f"❌ Build failed with code {result.returncode}"

        except subprocess.TimeoutExpired:
            build_status["output"] = "❌ Build timed out after 5 minutes"
            build_status["return_code"] = -1
            status_msg = "⏰ Build timed out"

        except Exception as e:
            build_status["output"] = f"❌ Error executing build: {str(e)}"
            build_status["return_code"] = -1
            status_msg = f"💥 Build error: {str(e)}"

        finally:
            build_status["running"] = False

        return status_msg

    # Build button and status display
    if product_selector and product_selector.value and build_mode:
        # Show current configuration
        config_info = f"""
        **Product**: {product_selector.value}  
        **Mode**: {build_mode.value}  
        **Target**: {profile_selector.value if profile_selector else "local"}
        """

        if model_selector and model_selector.value.strip():
            config_info += f"  \n**Selection**: `{model_selector.value.strip()}`"

        # Run button
        run_button = mo.ui.run_button(label="🚀 Execute Build")

        if run_button.value:
            if not build_status["running"]:
                result_msg = run_dbt_command()
                mo.md(f"""
                ### Build Configuration
                {config_info}
                
                {run_button}
                
                ### Build Result
                {result_msg}
                
                **Started**: {build_status["last_run"].strftime("%Y-%m-%d %H:%M:%S") if build_status["last_run"] else "Never"}  
                **Status**: {"🔄 Running" if build_status["running"] else "✅ Complete" if build_status["return_code"] == 0 else "❌ Failed"}
                """)
            else:
                mo.md(f"""
                ### Build Configuration
                {config_info}
                
                {run_button}
                
                ### Build Status
                🔄 **Build is currently running...**
                
                Started: {build_status["last_run"].strftime("%Y-%m-%d %H:%M:%S")}
                """)
        else:
            mo.md(f"""
            ### Build Configuration
            {config_info}
            
            {run_button}
            """)
    else:
        run_button = None
        mo.md("Complete the configuration above to execute a build.")
    return config_info, result_msg, run_button, run_dbt_command


@app.cell
def __(build_status, mo):
    # Build output display
    if build_status["output"]:
        mo.md(f"""
        ### Build Output
        
        **Return Code**: {build_status["return_code"]}
        
        ```
        {build_status["output"][:2000]}{"..." if len(build_status["output"]) > 2000 else ""}
        ```
        """)
    else:
        mo.md("")
    return


@app.cell
def __(mo, products_dir):
    # Quick product info
    def get_product_info(product_name):
        """Get basic info about a product."""
        product_path = products_dir / product_name

        info = {
            "path": str(product_path),
            "has_dbt_project": (product_path / "dbt_project.yml").exists(),
            "has_profiles": (product_path / "profiles.yml").exists(),
            "has_readme": (product_path / "README.md").exists(),
        }

        # Try to read dbt_project.yml for more info
        try:
            import yaml

            with open(product_path / "dbt_project.yml") as f:
                dbt_config = yaml.safe_load(f)
                info["dbt_version"] = dbt_config.get("require-dbt-version", "unknown")
                info["models"] = list(dbt_config.get("models", {}).keys())
        except:
            info["dbt_version"] = "unknown"
            info["models"] = []

        return info

    if product_selector and product_selector.value:
        product_info = get_product_info(product_selector.value)

        mo.md(f"""
        ---
        
        ### Product Information: {product_selector.value}
        
        - **Path**: `{product_info["path"]}`
        - **dbt Project**: {"✅" if product_info["has_dbt_project"] else "❌"} 
        - **Local Profiles**: {"✅" if product_info["has_profiles"] else "❌"}
        - **README**: {"✅" if product_info["has_readme"] else "❌"}
        - **dbt Version**: {product_info["dbt_version"]}
        """)
    else:
        mo.md("")
    return get_product_info, product_info


@app.cell
def __(mo):
    mo.md(
        r"""
        ---
        
        ## 💡 Usage Guide
        
        ### Build Modes
        - **Full Build**: Runs `dbt run` - executes models and tests
        - **Test Only**: Runs `dbt test` - executes data quality tests
        - **Compile Only**: Runs `dbt compile` - validates SQL without execution
        - **Parse Only**: Runs `dbt parse` - validates project structure
        
        ### Model Selection
        Use dbt selection syntax:
        - `+model_name` - model and all upstream dependencies
        - `model_name+` - model and all downstream dependents  
        - `tag:staging` - all models with "staging" tag
        - `--exclude test_type:unit` - exclude unit tests
        
        ### Environment Targets
        - **Local**: Uses local development settings
        - **Development**: Remote development environment
        - **Staging**: Pre-production testing environment
        - **Production**: Live production environment
        
        ### Prerequisites
        - dbt CLI must be installed and configured
        - Product must have `dbt_project.yml`
        - Database connections must be properly configured
        - Appropriate permissions for selected target environment
        
        ### Troubleshooting
        - Check build output for detailed error messages
        - Verify database connectivity and permissions
        - Ensure all required environment variables are set
        - Check that profiles.yml exists and is properly configured
        """
    )
    return


if __name__ == "__main__":
    app.run()
