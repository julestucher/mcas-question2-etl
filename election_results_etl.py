from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from time import sleep 
import json

# iterate over election items to obtain text
def scrape_precinct_data(town, precinct_element):

    # get response objects
    cols = precinct_element.find_elements(By.TAG_NAME, "td")

    # get data from the row
    data = {
        "town": town,
        "ward": cols[1].text,
        "precinct": cols[2].text,
        "response_yes": cols[3].text,
        "response_no": cols[4].text,
        "response_blank": cols[5].text,
        "response_total": cols[6].text,
    }

    # return data
    return(data)

# Scrape county Election Results
def scrape_county_data(**kwargs):

    # unwrap arguments
    county = kwargs['county']

    # initialize an instance of the browser
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    driver = webdriver.Firefox(options=firefox_options)

    # visit site for Question 2
    driver.get("https://electionstats.state.ma.us/ballot_questions/view/7391/filter_by_county:" + county)

    # sleep for 2 ms
    sleep(2)

    # expand all More buttons to view each district
    more_buttons = driver.find_elements(By.CSS_SELECTOR, ".expand_toggle")
    for button in more_buttons:
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center', inline: 'center'});", button)

        # try to click until visible
        while True:
            sleep(1)
            try:
                button.click()
            finally:
                break

    # get list of locality names and ids
    towns = driver.find_elements(By.CSS_SELECTOR, ".m_item")

    # init empty town results
    county_results = []

    # iterate over each town
    for town in towns:
        town_name = town.find_element(By.CSS_SELECTOR, ".label").text
        id = town.get_attribute('id').split("-")[-1]
        precincts = driver.find_elements(By.CSS_SELECTOR, ".precinct.precinct-for-" + id)
        precinct_results = [scrape_precinct_data(town_name, precinct_element) for precinct_element in precincts]
        county_results = county_results + precinct_results

    # close session and shut down browser
    driver.quit()

    # convert to data frame and return
    with open(f'/tmp/{county}.json', 'w') as f:
        f.write(json.dumps(county_results, indent=2))

def transform_county_data(**kwargs):

    # unwrap arguments
    county = kwargs['county']

    # read raw data
    election_df = pd.read_json(f'/tmp/{county}.json')

    # insert county name in column
    election_df.insert(0, 'county', county)

    # convert town to title case
    election_df['town'] = election_df['town'].str.title()
    
    # clean numeric strings
    numeric_cols = ['response_yes', 'response_no', 'response_blank', 'response_total']
    numeric_cols = election_df[numeric_cols].select_dtypes(include=['object']).columns
    election_df[numeric_cols] = election_df[numeric_cols].apply(lambda col: col.str.replace(',', '').astype(int))
    
    # write transformed data
    election_df.to_csv(f'/tmp/{county}_transformed.csv', index=False)



# create a function that will create operators for a specific county
def create_dag(dag_id, county):
    
    dag = DAG(dag_id, default_args=default_args, schedule_interval='@daily')

    extract_task = PythonOperator(
        task_id=f'scrape_{county}_data',
        provide_context=True,
        python_callable=scrape_county_data,
        op_kwargs={'county': county},
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id=f'transform_{county}_data',
        provide_context=True,
        python_callable=transform_county_data,
        op_kwargs={'county': county},
        dag=dag,
    )

    load_task = PostgresOperator(
        task_id=f'load_{county}_data',
        postgres_conn_id='mcas_db',
        sql=f"""
            COPY election_result (county, town, ward, precinct, response_yes, response_no, response_blank, response_total)
            FROM '/tmp/{county}_transformed.csv'
            DELIMITER ','
            CSV HEADER;
        """,
        dag=dag,
    )

    extract_task >> transform_task >> load_task

    return dag

# create config to store all counties
ma_counties = ['Barnstable', 'Berkshire', 'Bristol', 'Dukes', 'Essex', 'Franklin', 'Hampden', 'Hampshire', 'Middlesex', 'Nantucket', 'Norfolk', 'Plymouth', 'Suffolk', 'Worcester']

default_args = {
    'owner': 'jtucher',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# iterate over counties, creating a DAG for each
for county in ma_counties:
    dag_id = f"election_results_{county}"
    globals()[dag_id] = create_dag(dag_id, county)




