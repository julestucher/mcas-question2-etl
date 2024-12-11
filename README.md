# ETL Pipeline for 2024 Question 2 Polling and School Outcomes in Massachusetts

## Data Extraction Overview
In the 2024 election, Massachusetts Question 2 would repeal the state-wide MCAS high school graduation requirement and allow for districts to set graduation requirements for students. In my data engineering final project, I will pull together voting outcome data along with school district-level MCAS achievement data and graduation rates to create a data set for analysis. I will use Python to scrape the data from public MA Department of Elementary and Secondary Education (DESE) reports as well as from the Secretary of the Commonwealth of MA election statistics website. Finally, I will extract GIS data available from the Mass.gov website to link the town-level election restults with school district outcomes. This GIS data will also provide Shapefile geometries to create an interactive map output.

## Data Sources and Transformation Steps
- [MA DESE School and District Reports](https://profiles.doe.mass.edu/)
    - Two raw tables will be pulled, MCAS achievement and graduation rates
    - For MCAS achievement, raw data is at the district-subject level. Reshape wide to be at the district level.
    - Merge MCAS and graduation rates data at the district level.
    - Convert numeric columns to numeric data type.
- [Secretary of the Commonwealth of MA Election Results](https://electionstats.state.ma.us/ballot_questions/view/11621)
    - Text cleaning of county and town names.
    - Convert numeric columns to numeric data type.
- [MassGIS](https://www.mass.gov/info-details/massgis-data-public-school-districts)
    - Relevant data from Shapefile includes includes district codes, list of member towns for each district, and district Polygon.
    - Pull out district code and list of member towns. Reshape longer to create district-town lookup.
    - Pull out district code and Polygon object. Filter out any invalid geometries.

## Requirements / Tools Used
 - Python 3.8
 - Docker 27.3.1
 - Docker Compose v2.30.3
 - Apache Airflow
 - PostgreSQL
 - R
 - Firefox

## Setup

1. Configure PostgreSQL database according to the below section Database Schema.

2. Update `/dags/district_gis_etl.py` file on line 12 to include your database URI:
```
DB_URI = [YOUR DB URI]
```

2. Build custom Docker image:

```
docker build -t custom-airflow:latest .
```

3. Initialize Airflow using Docker.
```
docker compose up airflow-init
```

4. Run Airflow using Docker.
```
docker compose up
```

5. Add custom connection, either via Webserver UI or via CLI. Populate using your PostgreSQL details. If hosting PostgreSQL database locally, ensure that database is [open to connections from Docker container](https://stackoverflow.com/questions/31249112/allow-docker-container-to-connect-to-a-local-host-postgres-database).
```
docker compose run airflow-worker airflow connections add 'mcas_db' \
    --conn-uri [YOUR URI]
```

## Running the Pipeline

1. Access Airflow webserver running on port 8080. The username and password are `airflow`.

2. Trigger district GIS DAG.

3. Once confirming clean DAG run, data will be loaded into your PostgreSQL database and the analysis dashboard can be run.

4. Run webscraping ETLs locally.

## Running the Dashboard

1. Open `mcas-question2-etl.Rproj` in RStudio.

2.  Run dashboard from CLI.
```
R -e "shiny::runApp('./app.R')"
```

## Database Schema
```
# Define a database model for election results
CREATE TABLE election_result (
    id SERIAL PRIMARY KEY,
    county VARCHAR(100),
    town VARCHAR(100),
    response_yes NUMERIC,
    response_no NUMERIC,
    response_blank NUMERIC,
    response_total NUMERIC
);

# Define a database model for school district outcomes
CREATE TABLE school_district (
    id SERIAL PRIMARY KEY,
    district_code NUMERIC,
    district_name VARCHAR(100),
    year NUMERIC,
    num_meets_exceeds_ela NUMERIC,
    num_partial_meet_ela NUMERIC,
    num_not_meet_ela NUMERIC,
    percent_grad NUMERIC
);

# Define a database model for district-town linking
CREATE TABLE district_town_lookup (
    id SERIAL PRIMARY KEY,
    district_code NUMERIC,
    district_name VARCHAR(100),
    town VARCHAR(100)
);

# Define a database model for GIS data
CREATE TABLE district_shapes (
    id SERIAL PRIMARY KEY,
    district_code NUMERIC,
    district_name VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326)
);
```





