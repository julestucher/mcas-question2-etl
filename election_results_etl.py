import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from time import sleep
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

# define global for database URI
DB_URI = ''

def scrape_town_data(town):
    """
    Scrape election results for a single town.

    Args:
        town (Selenium element): element representing row of table for town

    Returns:
        type: dictionary containing town's election results
    """

    # get town name
    town_name = town.find_element(By.CSS_SELECTOR, ".label").text

    # get response objects
    cols = town.find_elements(By.TAG_NAME, "td")

    # get data from the row
    data = {
        "town": town_name,
        "response_yes": cols[1].text,
        "response_no": cols[2].text,
        "response_blank": cols[3].text,
        "response_total": cols[4].text,
    }

    # return data
    return(data)

def scrape_county_data(county):
    """
    Scrape election results for a county. Extract step of ETL. Handle Selenium web driver.
    
    Args:
        county (String): name of active county
    """

    # Initialize the WebDriver
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_service = Service(executable_path="./geckodriver")
    driver = webdriver.Firefox(options=firefox_options,
                               service = firefox_service)

    # visit site for Question 2
    driver.get("https://electionstats.state.ma.us/ballot_questions/view/11621/filter_by_county:" + county)

    # sleep for 2 ms
    sleep(2)

    # get list of locality names and ids
    towns = driver.find_elements(By.CSS_SELECTOR, ".m_item")

    # init empty town results
    county_results = []

    # iterate over each town
    for town in towns:
        town_results = scrape_town_data(town)
        county_results.append(town_results)

    # close session and shut down browser
    driver.quit()

    # convert to data frame and return
    return county_results

def transform_county_data(county_results, county):
    """
    Transform election results for a county, including cleaning and prep for load.
    
    Args:
        county (String): name of active county

    Return:
        DataFrame with transformed election results data
    """
    # convert raw data to dataframe
    election_df = pd.DataFrame(county_results)

    # insert county name in column
    election_df.insert(0, 'county', county)

    # convert town to title case
    election_df['town'] = election_df['town'].str.title()

    # clean strings with directional names (N/S/E/W)
    directions = {'N.':'North', 'S.':'South', 'E.':'East', 'W.':'West'}
    for key, val in directions:
        election_df['town'] = election_df['town'].apply(lambda col: col.replace(key, val))
    
    # clean numeric strings
    numeric_cols = ['response_yes', 'response_no', 'response_blank', 'response_total']
    numeric_cols = election_df[numeric_cols].select_dtypes(include=['object']).columns
    election_df[numeric_cols] = election_df[numeric_cols].apply(lambda col: col.str.replace(',', '').astype(int))
    
    # write transformed data
    return election_df

def load_county_data(election_df, county):
    """
    Delete and replace election results rows for county

    Args:
        - election_df (DataFrame): transformed election results data
        - county (String): name of active county
    """

    # connect directly to PostgreSQL DB and load new rows
    engine = create_engine(DB_URI)

    # delete pre-existing rows for county
    with Session(engine) as session:
        session.execute(text(f"""DELETE FROM election_result WHERE county = '{county}';"""))
    
    # append rows for county
    election_df.to_sql('election_result', engine, if_exists='append', index=False)

if __name__ == "__main__":

    if DB_URI == '':
        raise Exception("Please define database URI.")
    
    # Declare list of counties in Massachusetts
    ma_counties = ['Barnstable', 'Berkshire', 'Bristol', 'Dukes', 'Essex', 'Franklin', 'Hampden', 'Hampshire', 'Middlesex', 'Nantucket', 'Norfolk', 'Plymouth', 'Suffolk', 'Worcester']

    # iterate over counties, performing ETL for each
    for county in ma_counties:

        # Extract
        try:
            county_results = scrape_county_data(county)
        except:
            print(f"Extract task failed for {county} County")

        # Transform
        try:
            election_df = transform_county_data(county_results, county)
        except:
            print(f"Transform task failed for {county} County")

        # Load
        try:
            load_county_data(election_df, county)
        except:
            print(f"Load task failed for {county} County")

        # Print status to log
        print(f"ETL completed for {county} County")


