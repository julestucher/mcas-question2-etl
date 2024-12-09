# import required libraries
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from time import sleep 
import pandas as pd
from airflow import DAG
from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Scrape school outcome
def scrape_school_outcome(year, table_name, driver):

    # send year to filter
    year_filter = Select(driver.find_element(By.CSS_SELECTOR, "#ctl00_ContentPlaceHolder1_ddYear"))
    year_filter.select_by_value(str(year))

    # hit run report button 
    driver.find_element(By.CSS_SELECTOR, "#btnViewReport").click()

    # wait for report table
    table = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, table_name))
    )

    # get rows from table
    rows = table.find_elements(By.TAG_NAME, "tr")

    # get table header
    cells = rows[0].find_elements(By.TAG_NAME, "th")
    header = [cell.text for cell in cells]

    # iterate over rows
    data = []
    for row in rows[1:]:  # Skip header row
        cells = row.find_elements(By.TAG_NAME, "td")
        row_data = [cell.text for cell in cells]
        data.append(row_data)

    # convert to data frame
    df = pd.DataFrame(data, columns=header)
    df.insert(1, 'Year', year)

    return df

def scrape_district_data():

    # initialize an instance of the  browser
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    driver = webdriver.Firefox(options=firefox_options)

    # load MCAS webpage
    driver.get("https://profiles.doe.mass.edu/statereport/mcas.aspx")
    sleep(2)

    # filter to 10th grade
    grade_filter = driver.find_element(By.CSS_SELECTOR, "#ctl00_ContentPlaceHolder1_ddGrade")
    grade_filter.send_keys('1')

    # get 2023 MCAS results
    mcas_df = scrape_school_outcome(2023, "tblNetSchoolSpending", driver)
    mcas_df.to_csv('/tmp/mcas_results.csv', index=False)

    # load grad rate webpage
    driver.get("https://profiles.doe.mass.edu/statereport/gradrates.aspx")
    sleep(2)

    # get last year's grad rates
    grad_df = scrape_school_outcome(2023, "tblStateReport", driver)
    grad_df.to_csv('/tmp/grad_rate_results.csv', index=False)

    # close session and shut down browser
    driver.quit()

def transform_school_data():

    # read school data files
    mcas_df = pd.read_csv('/tmp/mcas_results.csv')
    grad_df = pd.read_csv('/tmp/grad_rate_results.csv')

    # subset rows to just the number in each passing group
    mcas_df = mcas_df[['District Code', 'Subject', 'M+E #', 'PM #', 'NM #']]
    mcas_df.columns = ['district_code', 'subject', 'num_meets_exceeds', 'num_partial_meet', 'num_not_meet']

    # convert datatypes
    numeric_cols = ['num_meets_exceeds', 'num_partial_meet', 'num_not_meet']
    mcas_df[numeric_cols] = mcas_df[numeric_cols].apply(lambda col: col.str.replace(',', '').astype(int))

    # reshape wide to the school-district level
    mcas_df = mcas_df.pivot(index = 'district_code',
                             columns='subject',
                             values = ['num_meets_exceeds', 'num_partial_meet', 'num_not_meet'])
    
    # Combine column names into a single index
    mcas_df.columns = ['_'.join(col).strip() for col in mcas_df.columns.values]
    mcas_df = mcas_df.reset_index()

    # subset and rename cols of grad data
    grad_df = grad_df[['District Name', 'District Code', 'Year', '% Graduated']]
    grad_df.columns = ['district_name', 'district_code', 'year', 'percent_grad']
    grad_df['percent_grad'] = grad_df['percent_grad'].astype(float)

    # merge to create school-based data
    school_df = pd.merge(mcas_df, grad_df, on = 'district_code', validate = '1:1')

    # filter out state-wide results
    school_df = school_df[school_df['district_code'] != 0]
    numeric_cols = ['district_code', 'year', 'num_meets_exceeds_ELA', 'num_partial_meet_ELA', 'num_not_meet_ELA']
    school_df[numeric_cols] = school_df[numeric_cols].apply(lambda col: col.astype(int))
    school_df = school_df[['district_code', 'district_name', 'year', 'num_meets_exceeds_ELA', 'num_partial_meet_ELA', 'num_not_meet_ELA', 'percent_grad']]

    # write transformed data
    school_df.to_csv(f'/tmp/school_outcomes_transformed.csv', index=False)

default_args = {
    'owner': 'jtucher',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Create DAG
dag = DAG('school_outcomes_etl', default_args=default_args, schedule_interval='@daily')

extract_task = PythonOperator(
    task_id='scrape_district_data',
    python_callable=scrape_district_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_school_data',
    python_callable=transform_school_data,
    dag=dag,
)

load_task = SQLExecuteQueryOperator(
    task_id='load_school_data',
    conn_id='mcas_db',
    sql="""
    DELETE FROM school_district;
    COPY school_district (district_code, district_name, year, num_meets_exceeds_ela, num_partial_meet_ela, num_not_meet_ela, percent_grad)
    FROM '/tmp/school_outcomes_transformed.csv'
    DELIMITER ','
    CSV HEADER;
    """,
    dag=dag,
)

extract_task >> transform_task >> load_task