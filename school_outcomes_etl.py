# import required libraries
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from time import sleep 
import pandas as pd
from sqlalchemy import create_engine

# define global for database URI
DB_URI = 'postgresql://juliatucher:dataeng690@localhost:5432/juliatucher'

def scrape_school_outcome(year, table_name, driver):
    """
    Scrape single outcome for all school districts. Flexible for MCAS or graduation rates.

    Args:
        year (Int): Year of requested data.
        table_name (String): Name of table. Specific to MCAS achievement or graduation rates.
        driver (Firefox Selenium driver): current driver.

    Returns:
        type: Pandas dataframe containing school outcomes data
    """
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
    """
    Scrape all school outcomes. Extract step of ETL. Handles Selenium webdriver.

    Returns:
        - DataFrame with MCAS results
        - DataFrame with graduation rates
    """

    # Initialize the WebDriver
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

    # load grad rate webpage
    driver.get("https://profiles.doe.mass.edu/statereport/gradrates.aspx")
    sleep(2)

    # get last year's grad rates
    grad_df = scrape_school_outcome(2023, "tblStateReport", driver)

    # close session and shut down browser
    driver.quit()

    return mcas_df, grad_df

def transform_district_data(mcas_df, grad_df):
    """
    Transform raw school district outcome files. Prepare for load into database.
    """

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
    return school_df

def load_district_data(school_df):
    """
    Delete and replace school outcomes

    Args:
        - school_df (DataFrame): transformed school outcomes data
    """

    # connect directly to PostgreSQL DB and load
    engine = create_engine(DB_URI)
    school_df.to_sql('school_district', engine, if_exists='replace', index=False)

if __name__ == "__main__":

    # Extract
    try:
        mcas_df, grad_df = scrape_district_data()
    except:
        print("Extract task failed for school district data")

    # Transform
    try:
        school_df = transform_district_data(mcas_df, grad_df)
    except:
        print("Transform task failed for school district data")

    # Load
    try:
        load_district_data(school_df)
    except:
        print("Load task failed for school district data")
