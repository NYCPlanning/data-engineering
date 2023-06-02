from helper.engines import build_engine, psycopg2_connect
import os
import io
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine

# Create psycopg2 connections
con = build_engine
db_connection = psycopg2_connect(con.url)
db_cursor = db_connection.cursor()
str_buffer = io.StringIO()

# Load manual mapping data
geojson_filename = "cbbr_manualgeoms_12172020.geojson"
geojson_path = Path(__file__).resolve().parent.parent / "geometries" / geojson_filename
gdf = pd.DataFrame(gpd.read_file(geojson_path))[["unique_id", "geometry"]]
gdf = gdf.rename(columns={"geometry": "geom"})
gdf.loc[:, "geom"] = gdf["geom"].apply(lambda x: f"SRID=4326;{x}" if x else "")
print("Loading manually mapped geometries: \n", gdf.head())

# Create table
create = f"""
DROP TABLE IF EXISTS manual_geoms.\"FY22\";
CREATE TABLE manual_geoms.\"FY22\" (
    unique_id text,
    geom geometry 
);
"""

con.connect().execute(create)
con.dispose()

# Export modified table to CSV, and copy to postgres
gdf.to_csv(str_buffer, sep="/", header=False, index=False)
str_buffer.seek(0)
db_cursor.copy_from(str_buffer, 'manual_geoms."FY22"', sep="/", null="")
db_cursor.connection.commit()

# Close connections
str_buffer.close()
db_cursor.close()
db_connection.close()
