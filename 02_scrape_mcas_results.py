# import required libraries
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from time import sleep 
import pandas as pd

# Scrape county Election Results
def scrape_year_results(year, table_name, driver):

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

def scrape_district_results():

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

    # get 4 years of MCAS results
    #mcas_df = scrape_year_results(2024, "tblNetSchoolSpending", driver)

    # collapse to single data frame
    #mcas_df = pd.concat(mcas_results, ignore_index=True)
    #mcas_df.to_csv('mcas_results.csv', index=False)

    # load grad rate webpage
    driver.get("https://profiles.doe.mass.edu/statereport/gradrates.aspx")
    sleep(2)

    # get last year's grad rates
    grad_df = scrape_year_results(2023, "tblStateReport", driver)
    grad_df.to_csv('grad_rate_results.csv', index=False)

    # close session and shut down browser
    driver.quit()

    return 


# scrape data from election results webpage
scrape_district_results()


