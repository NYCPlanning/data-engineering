from setuptools import setup

setup(
    name="src",
    version="0.1",
    packages=["src"],
    install_requires=[
        "sqlalchemy",
        "psycopg2-binary",
        "pandas",
        "numpy",
        "streamlit",
        "plotly",
        "python-dotenv",
        "boto3",
        "streamlit-aggrid",
        "geopandas",
        "matplotlib",
        "python-abc",
        "numerize",
    ],
    zip_safe=False,
)
