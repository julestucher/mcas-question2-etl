# import required libraries
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from time import sleep 
import pandas as pd

# init Flask app
app = Flask(__name__)

# SQLite DB setup
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///election_results.db'
db = SQLAlchemy(app)

# Define constants
ma_counties = ['Barnstable', 'Berkshire', 'Bristol', 'Dukes', 'Essex', 'Franklin', 'Hampden', 'Hampshire', 'Middlesex', 'Nantucket', 'Norfolk', 'Plymouth', 'Suffolk', 'Worcester']

# Define a database model for election results
class ElectionResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    question_number = db.Column(db.Integer, nullable=False)
    question_text = db.Column(db.String(1000), nullable=False)
    question_type = db.Column(db.String(100), nullable=False)
    district = db.Column(db.String(100), nullable=False)
    results_yes = db.Column(db.Integer, nullable=False)
    results_no = db.Column(db.Integer, nullable=False)
    results_blank = db.Column(db.Integer, nullable=False)
    results_total = db.Column(db.Integer, nullable=False)

# Create the database
with app.app_context():
    db.create_all()

# Preprocess CSV of data
def preprocess_data(df):
    return

# iterate over election items to obtain text
def scrape_items(town, precinct):

    # get response objects
    cols = precinct.find_elements(By.TAG_NAME, "td")

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
def scrape_county_results(county, driver):

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
        precinct_results = [scrape_items(town_name, precinct) for precinct in precincts]
        county_results = county_results + precinct_results

    # convert to data frame and return
    county_df = pd.DataFrame(county_results)
    county_df.insert(0, 'county', county)
    return county_df

def scrape_election_results():

    # initialize an instance of the browser
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    driver = webdriver.Firefox(options=firefox_options)

    # return results for all counties
    results = [scrape_county_results(county, driver) for county in ma_counties]

    # collapse to single data frame
    results_df = pd.concat(results, ignore_index=True)
    results_df.to_csv('election_results.csv', index=False)


    # close session and shut down browser
    driver.quit()

    return 


# scrape data from election results webpage
scrape_election_results()


