from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import requests
import geopandas as gpd
import zipfile
import os
from sqlalchemy import create_engine

# define global for database URI
DB_URI = 'postgresql://juliatucher:dataeng690@host.docker.internal:5432/juliatucher'

def fetch_district_geo_data():
    """
    Download GIS data from MA gov site. Extract step of ETL.
    """

    # Request ZIP file from source
    url = 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/schooldistricts.zip'
    response = requests.get(url)
    zip_path = '/tmp/shapefile.zip'

    # Save the zip file to tmp directory
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    # Extract the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('/tmp/shapefiles')

    # Clean up the zip file
    os.remove(zip_path)

def transform_district_geo_data():
    """
    Transform district SHP file to create school district-town lookup. This will be loaded into the database and
    used to connect school district outcomes with election results.
    """

    # open downloaded SHP file for school/regional districts
    gdf_raw = gpd.read_file('/tmp/shapefiles/SCHOOLDISTRICTS_POLY.shp')

    # select columns for school district/town crosswalk, reshape long
    sd_town_cols = gdf_raw[['ORG8CODE', 'DISTRICT_N', 'MEMBERLIST']]
    sd_town_cols['MEMBERLIST'] = sd_town_cols['MEMBERLIST'].str.split(', ')
    cw_df = sd_town_cols.explode('MEMBERLIST')
    cw_df.columns = ['district_code', 'district_name', 'town']

    # set town to district name if missing
    cw_df.loc[cw_df['town'].isna(), 'town'] = cw_df.loc[cw_df['town'].isna(), 'district_name']
    
    # write transformed data
    cw_df.to_csv(f'/tmp/district_town_crosswalk.csv', index=False)

def load_district_shapes_to_postgis():
    """
    Load GIS columns of school district Shapefile data into database directly. 
    """

    # open downloaded SHP file for school/regional districts
    gdf_raw = gpd.read_file('/tmp/shapefiles/SCHOOLDISTRICTS_POLY.shp')
    
    # select columns for school district geo data
    gdf = gdf_raw[['ORG8CODE', 'DISTRICT_N', 'geometry']]
    gdf.columns = ['district_code', 'district_name', 'geometry']
    gdf.loc[:,'geometry'] = gdf.loc[:,'geometry'].to_crs(epsg=4326)

    # filter out invalid geometries
    gdf = gdf[gdf['geometry'].is_valid]

    # connect directly to PostgreSQL DB and load
    engine = create_engine(DB_URI)
    gdf.to_postgis('district_shapes', engine, if_exists='replace', index=False)

# Set default DAG arguments
default_args = {
    'owner': 'jtucher',
    'start_date': datetime(2024, 12, 9),
    'retries': 1,
}

# Create DAG for district GIS ETL
dag = DAG('district_gis_etl', default_args=default_args, schedule='@daily')

# Declare extract task
extract_task = PythonOperator(
    task_id='fetch_district_geo_data',
    python_callable=fetch_district_geo_data,
    dag=dag,
)

# Declare transform task
transform_task = PythonOperator(
    task_id='transform_district_geo_data',
    python_callable=transform_district_geo_data,
    dag=dag,
)

# Declare first load task (for crosswalk)
load_cw_task = SQLExecuteQueryOperator(
    task_id='load_district_cw_data',
    conn_id='mcas_db',
    sql="""
    DELETE FROM district_town_lookup;
    COPY district_town_lookup (district_code, district_name, town)
    FROM '/tmp/district_town_crosswalk.csv'
    DELIMITER ','
    CSV HEADER;
    """,
    dag=dag,
)

# Declare second load task (for GIS data)
load_gis_task = PythonOperator(
    task_id='load_district_shapes_to_postgis',
    python_callable=load_district_shapes_to_postgis,
)

extract_task >> transform_task >> load_cw_task >> load_gis_task
