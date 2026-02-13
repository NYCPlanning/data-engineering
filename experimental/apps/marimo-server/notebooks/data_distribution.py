import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo

    mo.md(
        r"""
        # 📊 Data Distribution Dashboard
        
        This notebook provides an interface to view and manage data distribution tasks
        for NYC DCP data products, including publishing to open data portals and 
        internal distribution channels.
        """
    )
    return (mo,)


@app.cell
def __(mo):
    import json
    import subprocess
    import sys
    from datetime import datetime, timedelta
    from pathlib import Path
    import os

    # Repository paths
    repo_root = Path(__file__).parents[3]
    products_dir = repo_root / "products"

    mo.md(f"📁 **Repository Root**: `{repo_root}`")
    return Path, datetime, json, os, products_dir, repo_root, subprocess, sys, timedelta


@app.cell
def __(mo, products_dir):
    # Discover products with distribution configs
    def find_distributable_products():
        """Find products that have distribution configurations."""
        products = []

        if not products_dir.exists():
            return products

        for product_dir in products_dir.iterdir():
            if not product_dir.is_dir():
                continue

            product_info = {
                "name": product_dir.name,
                "path": product_dir,
                "has_dbt": (product_dir / "dbt_project.yml").exists(),
                "has_recipe": (product_dir / "recipe.yml").exists(),
                "has_build_config": (product_dir / "build.yml").exists(),
                "distribution_channels": [],
            }

            # Check for common distribution indicators
            if (product_dir / "publish").exists():
                product_info["distribution_channels"].append("Custom Publisher")
            if (product_dir / "socrata").exists():
                product_info["distribution_channels"].append("Socrata/Open Data")
            if (product_dir / "s3").exists():
                product_info["distribution_channels"].append("S3")
            if (product_dir / "ftp").exists():
                product_info["distribution_channels"].append("FTP")

            products.append(product_info)

        return sorted(products, key=lambda x: x["name"])

    distributable_products = find_distributable_products()

    if distributable_products:
        mo.md(f"""
        ## Available Products for Distribution ({len(distributable_products)})
        """)
    else:
        mo.md("❌ No products found with distribution configurations")
    return distributable_products, find_distributable_products


@app.cell
def __(distributable_products, mo):
    # Product selector with distribution info
    if distributable_products:
        # Create options with distribution channel info
        product_options = []
        for product in distributable_products:
            channels = (
                ", ".join(product["distribution_channels"])
                if product["distribution_channels"]
                else "None configured"
            )
            label = f"{product['name']} ({channels})"
            product_options.append((label, product["name"]))

        product_selector = mo.ui.dropdown(
            options=product_options, label="Select Product:"
        )

        mo.md(f"{product_selector}")
    else:
        product_selector = None
    return product_options, product_selector


@app.cell
def __(distributable_products, mo, product_selector):
    # Show selected product details
    if product_selector and product_selector.value:
        selected_product = next(
            (p for p in distributable_products if p["name"] == product_selector.value),
            None,
        )

        if selected_product:
            channels_list = (
                "\n".join(
                    [f"- {ch}" for ch in selected_product["distribution_channels"]]
                )
                or "- No distribution channels configured"
            )

            mo.md(f"""
            ### Selected Product: {selected_product["name"]}
            
            **Path**: `{selected_product["path"]}`  
            **dbt Project**: {"✅" if selected_product["has_dbt"] else "❌"}  
            **Recipe Config**: {"✅" if selected_product["has_recipe"] else "❌"}  
            **Build Config**: {"✅" if selected_product["has_build_config"] else "❌"}  
            
            **Distribution Channels**:
            {channels_list}
            """)
        else:
            selected_product = None
            mo.md("❌ Product not found")
    else:
        selected_product = None
        mo.md("")
    return channels_list, selected_product


@app.cell
def __(mo, selected_product):
    # Distribution action selector
    if selected_product:
        action_selector = mo.ui.dropdown(
            options=[
                ("Check Status", "status"),
                ("Publish Latest", "publish"),
                ("Validate Data", "validate"),
                ("View Distribution Log", "log"),
                ("Force Republish", "force_publish"),
            ],
            label="Distribution Action:",
        )

        # Environment selector
        env_selector = mo.ui.dropdown(
            options=[
                ("Development", "dev"),
                ("Staging", "staging"),
                ("Production", "prod"),
            ],
            value="dev",
            label="Target Environment:",
        )

        mo.md(f"""
        ### Distribution Actions
        
        {action_selector}
        {env_selector}
        """)
    else:
        action_selector = env_selector = None
    return action_selector, env_selector


@app.cell
def __(mo):
    # Mock distribution status data
    def get_mock_distribution_status(product_name, environment):
        """Generate mock distribution status for demonstration."""
        from datetime import datetime, timedelta
        import random

        # Mock data for different products and environments
        base_time = datetime.now() - timedelta(hours=random.randint(1, 48))

        status = {
            "product": product_name,
            "environment": environment,
            "last_published": base_time.strftime("%Y-%m-%d %H:%M:%S"),
            "version": f"2024v{random.randint(1, 5)}.{random.randint(0, 10)}",
            "status": random.choice(
                ["success", "success", "success", "warning", "error"]
            ),
            "channels": [],
        }

        # Mock channel statuses
        channels = ["Socrata Open Data", "S3 Bucket", "Internal FTP", "API Endpoint"]
        for channel in random.sample(channels, random.randint(1, 3)):
            channel_status = {
                "name": channel,
                "status": random.choice(
                    ["published", "published", "pending", "failed"]
                ),
                "last_update": (
                    base_time + timedelta(minutes=random.randint(0, 60))
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "records_count": random.randint(1000, 100000),
                "file_size": f"{random.randint(1, 500)} MB",
            }
            status["channels"].append(channel_status)

        return status

    mo.md("## Distribution Status")
    return (get_mock_distribution_status,)


@app.cell
def __(
    action_selector, env_selector, get_mock_distribution_status, mo, selected_product
):
    # Execute distribution actions
    if selected_product and action_selector and action_selector.value:
        if action_selector.value == "status":
            # Show distribution status
            status = get_mock_distribution_status(
                selected_product["name"], env_selector.value if env_selector else "dev"
            )

            # Status emoji mapping
            status_emoji = {
                "success": "✅",
                "warning": "⚠️",
                "error": "❌",
                "published": "✅",
                "pending": "🔄",
                "failed": "❌",
            }

            channels_info = ""
            for channel in status["channels"]:
                emoji = status_emoji.get(channel["status"], "❓")
                channels_info += f"""
                **{channel["name"]}** {emoji}
                - Status: {channel["status"]}
                - Last Update: {channel["last_update"]}
                - Records: {channel["records_count"]:,}
                - Size: {channel["file_size"]}
                
                """

            overall_emoji = status_emoji.get(status["status"], "❓")

            mo.md(f"""
            ### Distribution Status {overall_emoji}
            
            **Product**: {status["product"]}  
            **Environment**: {status["environment"]}  
            **Version**: {status["version"]}  
            **Last Published**: {status["last_published"]}  
            **Overall Status**: {status["status"]} {overall_emoji}
            
            #### Distribution Channels
            
            {channels_info}
            """)

        elif action_selector.value == "publish":
            # Mock publish action
            execute_button = mo.ui.run_button(label="🚀 Execute Publish")

            if execute_button.value:
                mo.md(f"""
                ### Publishing {selected_product["name"]}
                
                {execute_button}
                
                **Environment**: {env_selector.value if env_selector else "dev"}
                
                🔄 **Publishing in progress...**
                
                1. ✅ Validating data quality
                2. ✅ Preparing distribution packages  
                3. 🔄 Publishing to Socrata Open Data
                4. ⏳ Uploading to S3 bucket
                5. ⏳ Updating API endpoints
                
                *This is a mock execution for demonstration purposes.*
                """)
            else:
                mo.md(f"""
                ### Publish {selected_product["name"]}
                
                **Target Environment**: {env_selector.value if env_selector else "dev"}
                
                This will publish the latest version of the data product to all configured distribution channels.
                
                {execute_button}
                """)

        elif action_selector.value == "validate":
            # Mock validation
            mo.md(f"""
            ### Data Validation Results
            
            **Product**: {selected_product["name"]}
            **Environment**: {env_selector.value if env_selector else "dev"}
            
            #### Validation Checks
            
            ✅ **Schema Validation** - All required columns present  
            ✅ **Data Quality** - No null values in required fields  
            ✅ **Row Count** - Expected number of records (47,392)  
            ⚠️ **Freshness Check** - Data is 2 days old (acceptable)  
            ❌ **Geometry Validation** - 12 invalid geometries found  
            
            #### Recommendations
            - Review and fix invalid geometries before publishing
            - Consider data refresh if freshness is critical
            
            *Mock validation results for demonstration.*
            """)

        elif action_selector.value == "log":
            # Mock distribution log
            mo.md(f"""
            ### Distribution Log
            
            **Product**: {selected_product["name"]}  
            **Environment**: {env_selector.value if env_selector else "dev"}
            
            ```
            2024-11-13 14:30:15 [INFO] Starting distribution process
            2024-11-13 14:30:16 [INFO] Validating data quality...
            2024-11-13 14:30:18 [INFO] Quality checks passed
            2024-11-13 14:30:19 [INFO] Publishing to Socrata (dataset: abc-123)
            2024-11-13 14:31:45 [INFO] Socrata publish completed (47,392 records)
            2024-11-13 14:31:46 [INFO] Uploading to S3: s3://bucket/product/latest/
            2024-11-13 14:32:12 [INFO] S3 upload completed
            2024-11-13 14:32:13 [INFO] Updating API metadata
            2024-11-13 14:32:15 [INFO] Distribution process completed successfully
            2024-11-13 14:32:16 [INFO] Notification sent to subscribers
            ```
            
            *Mock log output for demonstration.*
            """)

        else:
            mo.md("Action not implemented yet.")

    else:
        mo.md("Select a product and action to proceed.")
    return channels_info, execute_button, overall_emoji, status, status_emoji


@app.cell
def __(mo):
    # Distribution schedule and automation info
    mo.md(
        r"""
        ---
        
        ## 📅 Distribution Schedule
        
        ### Automated Distributions
        
        | Product | Environment | Schedule | Last Run | Status |
        |---------|-------------|----------|----------|---------|
        | PLUTO | Production | Daily 6AM | 2024-11-13 06:15 | ✅ Success |
        | LION | Production | Weekly Mon | 2024-11-11 06:30 | ✅ Success |
        | Facilities | Staging | On-demand | 2024-11-12 14:20 | ⚠️ Warning |
        | ZAP | Production | Monthly | 2024-11-01 07:00 | ✅ Success |
        
        ### Distribution Channels
        
        - **Socrata Open Data**: NYC Open Data portal for public datasets
        - **S3 Buckets**: Internal and external file distribution
        - **FTP Servers**: Legacy system integration  
        - **API Endpoints**: Real-time data access
        - **Internal Databases**: Cross-system data sharing
        
        *Mock data for demonstration purposes.*
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        ---
        
        ## 💡 Usage Guide
        
        ### Distribution Actions
        
        - **Check Status**: View current publication status across all channels
        - **Publish Latest**: Distribute the most recent version of data
        - **Validate Data**: Run quality checks before distribution
        - **View Distribution Log**: See detailed execution logs
        - **Force Republish**: Override and republish existing data
        
        ### Distribution Channels
        
        #### Socrata/Open Data Portal
        - Public-facing datasets for transparency
        - Automated metadata and schema updates
        - Version control and change tracking
        
        #### S3 Distribution
        - Bulk file downloads (CSV, GeoJSON, Shapefile)
        - Automated backup and archival
        - Cross-region replication
        
        #### API Endpoints
        - Real-time data access
        - Authentication and rate limiting
        - Standardized REST/GraphQL interfaces
        
        ### Environment Management
        
        - **Development**: Testing and validation
        - **Staging**: Pre-production verification  
        - **Production**: Live public distribution
        
        ### Monitoring and Alerts
        
        - Automated quality validation
        - Publication failure notifications
        - Performance monitoring and metrics
        - Data freshness alerts
        
        ### Prerequisites
        
        - Product must have completed successful build
        - Distribution channels must be configured
        - Appropriate permissions for target environment
        - Valid data quality validation results
        """
    )
    return


if __name__ == "__main__":
    app.run()
