import marimo

__generated_with = "0.9.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo

    mo.md(
        r"""
        # 🔍 Data Product Version Checker
        
        This notebook provides an interface to check available versions of data products 
        across the NYC DCP data engineering pipeline.
        """
    )
    return (mo,)


@app.cell
def __(mo):
    # Import version utilities from dcpy
    import sys
    from pathlib import Path

    # Add dcpy to path if needed
    repo_root = Path(__file__).parents[
        3
    ]  # Go up from marimo-server/notebooks to data-engineering root
    dcpy_path = repo_root / "dcpy"
    if str(dcpy_path) not in sys.path:
        sys.path.insert(0, str(dcpy_path))

    try:
        from dcpy.utils.versions import (
            Version,
            Date,
            MajorMinor,
            CapitalBudget,
            DateVersionFormat,
        )
        from dcpy.utils.versions import CapitalBudgetRelease

        mo.md("✅ Successfully imported version utilities from dcpy")
    except ImportError as e:
        mo.md(f"❌ Failed to import from dcpy: {e}")
    return (
        CapitalBudget,
        CapitalBudgetRelease,
        Date,
        DateVersionFormat,
        MajorMinor,
        Path,
        Version,
        dcpy_path,
        repo_root,
        sys,
    )


@app.cell
def __(mo):
    # Product selector
    product_selector = mo.ui.dropdown(
        options=[
            "lion",
            "facilities",
            "zap",
            "ceqr",
            "pluto",
            "developments",
            "sca_capacity_projects",
        ],
        value="lion",
        label="Select Data Product:",
    )

    mo.md(f"## Select Product\n{product_selector}")
    return (product_selector,)


@app.cell
def __(mo, product_selector):
    # Version format selector
    format_selector = mo.ui.dropdown(
        options=[
            ("Date", "date"),
            ("Month", "month"),
            ("Quarter", "quarter"),
            ("Fiscal Year", "fiscal_year"),
            ("Major/Minor", "major_minor"),
            ("Capital Budget", "capital_budget"),
        ],
        value="date",
        label="Version Format:",
    )

    if product_selector.value:
        mo.md(f"### {product_selector.value.title()} Versions\n{format_selector}")
    else:
        mo.md("Please select a product first.")
    return (format_selector,)


@app.cell
def __(
    CapitalBudget,
    CapitalBudgetRelease,
    Date,
    DateVersionFormat,
    MajorMinor,
    format_selector,
    mo,
    product_selector,
):
    # Generate example versions based on selected format
    from datetime import date, timedelta
    import random

    def generate_example_versions(format_type, product_name, count=5):
        """Generate example versions for demonstration."""
        versions = []

        if format_type == "date":
            base_date = date.today()
            for i in range(count):
                version_date = base_date - timedelta(days=i * 30)  # Monthly releases
                versions.append(Date(version_date, DateVersionFormat.date))

        elif format_type == "month":
            base_date = date.today().replace(day=1)
            for i in range(count):
                version_date = base_date - timedelta(days=i * 32)  # Monthly
                versions.append(Date(version_date, DateVersionFormat.month))

        elif format_type == "quarter":
            base_date = date.today()
            for i in range(count):
                # Go back by quarters
                quarter_date = date(base_date.year, max(1, base_date.month - i * 3), 1)
                versions.append(Date(quarter_date, DateVersionFormat.quarter))

        elif format_type == "fiscal_year":
            current_year = date.today().year
            for i in range(count):
                fy_date = date(current_year - i, 7, 1)  # July 1st start of FY
                versions.append(Date(fy_date, DateVersionFormat.fiscal_year))

        elif format_type == "major_minor":
            current_year = date.today().year % 100  # 2-digit year
            for i in range(count):
                major = random.randint(1, 5)
                minor = random.randint(0, 3) if random.random() > 0.3 else 0
                versions.append(MajorMinor(current_year, major, minor))

        elif format_type == "capital_budget":
            current_year = date.today().year % 100  # 2-digit year
            for i in range(count):
                year = current_year - (i // 3)  # New year every 3 versions
                release_idx = (2 - (i % 3)) + 1  # Cycle through releases backwards
                release = CapitalBudgetRelease(min(3, max(1, release_idx)))
                versions.append(CapitalBudget(year, release))

        return sorted(versions, reverse=True)  # Most recent first

    if product_selector.value and format_selector.value:
        example_versions = generate_example_versions(
            format_selector.value, product_selector.value
        )

        # Display versions in a table
        version_data = [
            {"Version": v.label, "Type": type(v).__name__, "Sort Order": i + 1}
            for i, v in enumerate(example_versions)
        ]

        mo.md(f"""
        ### Available Versions for {product_selector.value.title()}
        
        **Format**: {format_selector.value.replace("_", " ").title()}
        
        {mo.ui.table(version_data, selection=None)}
        
        *Note: These are example versions for demonstration. In production, 
        these would be fetched from your actual data product registry.*
        """)
    else:
        mo.md("Please select both a product and version format.")
    return (
        date,
        example_versions,
        generate_example_versions,
        random,
        timedelta,
        version_data,
    )


@app.cell
def __(example_versions, mo, product_selector):
    # Version comparison tool
    if len(example_versions) >= 2:
        version1_selector = mo.ui.dropdown(
            options=[(v.label, i) for i, v in enumerate(example_versions)],
            label="First Version:",
        )

        version2_selector = mo.ui.dropdown(
            options=[(v.label, i) for i, v in enumerate(example_versions)],
            value=1 if len(example_versions) > 1 else 0,
            label="Second Version:",
        )

        mo.md(f"""
        ### Version Comparison Tool
        
        Compare two versions of **{product_selector.value}**:
        
        {version1_selector}
        {version2_selector}
        """)
    else:
        version1_selector = None
        version2_selector = None
        mo.md("")
    return version1_selector, version2_selector


@app.cell
def __(example_versions, mo, version1_selector, version2_selector):
    # Perform version comparison
    if (
        version1_selector
        and version2_selector
        and version1_selector.value is not None
        and version2_selector.value is not None
    ):
        v1 = example_versions[version1_selector.value]
        v2 = example_versions[version2_selector.value]

        comparison_result = "equal" if v1 == v2 else ("newer" if v1 > v2 else "older")

        emoji_map = {"newer": "📈", "older": "📉", "equal": "🟰"}

        mo.md(f"""
        ### Comparison Result
        
        {emoji_map[comparison_result]} **{v1.label}** is **{comparison_result}** than **{v2.label}**
        
        - Version 1: `{v1.label}` ({type(v1).__name__})
        - Version 2: `{v2.label}` ({type(v2).__name__})
        """)
    else:
        mo.md("")
    return comparison_result, emoji_map, v1, v2


@app.cell
def __(mo):
    # Instructions and tips
    mo.md(
        r"""
        ---
        
        ## 💡 Usage Tips
        
        - **Version Selection**: Choose different products and formats to see how versions are structured
        - **Comparison Tool**: Compare any two versions to understand chronological ordering
        - **Integration**: This interface can be extended to connect to actual data product registries
        
        ### Supported Version Types
        
        - **Date**: Full date versions (YYYY-MM-DD)
        - **Month**: Monthly versions (YYYY-MM) 
        - **Quarter**: Quarterly versions (YYQ#)
        - **Fiscal Year**: Annual fiscal year versions (FYYYY)
        - **Major/Minor**: Semantic versioning (YYv#.#.#)
        - **Capital Budget**: NYC capital budget releases (YYprelim/exec/adopt)
        
        ### Next Steps
        
        To make this production-ready:
        1. Connect to actual data product metadata
        2. Add version creation/tagging functionality
        3. Integrate with build and deployment pipelines
        4. Add version deprecation and lifecycle management
        """
    )
    return


if __name__ == "__main__":
    app.run()
