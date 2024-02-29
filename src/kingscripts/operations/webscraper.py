## Developer: Gabe Tatman
## Date: 2024-02-27
## Description: This is a script that scrapes data from the web and writes it to a sharepoint file

# Importing Python Packages
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os

import datetime
import time

username = str(os.getenv('PDS_USERNAME'))
password = str(os.getenv('PDS_PASSWORD'))
pathToECHO2250 = str(os.getenv("ECHO2250"))
pathToECHO2251 = str(os.getenv("ECHO2251"))
pathToECHO2252 = str(os.getenv("ECHO2252"))

def download_echo():
    # Function to download file with specified options
    def download_file(download_directory, button_value, desired_filename):
        options = Options()
        options.add_experimental_option("detach", False)  # Keep the window open after the script finishes
        options.add_experimental_option('prefs', {
            'download.default_directory': download_directory,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True,
            'download.default_filename': desired_filename
        })
        # Run headless
        options.add_argument("--headless=new")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        # Link to Data Portal
        driver.get("https://pdswdx.com/login/")
        driver.find_element(By.NAME,"ctl00$ctl00$ctl00$ctl00$ContentPlaceHolder1$FullColumn$RightColumn$RightColumn$tbUsername").send_keys(username)
        driver.find_element(By.NAME,"ctl00$ctl00$ctl00$ctl00$ContentPlaceHolder1$FullColumn$RightColumn$RightColumn$tbPassword").send_keys(password)
        driver.find_element(By.NAME,"ctl00$ctl00$ctl00$ctl00$ContentPlaceHolder1$FullColumn$RightColumn$RightColumn$btnSubmit").click()

        element_with_href = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[href="/Production/Daily"]'))
        )

        driver.find_element(By.CSS_SELECTOR,'[href="/Production/Daily"]').click()
        driver.find_element(By.CSS_SELECTOR,f'[value="{button_value}"]').click()
        #time.sleep(2)  # Adding a delay to ensure the download starts
        driver.find_element(By.NAME,"ctl00$ctl00$ctl00$ctl00$ContentPlaceHolder1$FullColumn$RightColumn$RightColumn$accTypeSelect_content$btnAddCustomSelection").click()
        #time.sleep(2)  # Adding a delay to ensure the download starts
        driver.find_element(By.NAME,"ctl00$ctl00$ctl00$ctl00$ContentPlaceHolder1$FullColumn$RightColumn$RightColumn$accTypeSelect_content$btnCsv").click()
        time.sleep(1)  # Adding a delay to ensure the download starts

        # Close the browser
        driver.quit()

    # Generate timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Specify desired filenames and button values
    files_info = [
        {"button_value": "728995", "filename": f"echo_2250_{timestamp}.csv", "path": pathToECHO2250},
        {"button_value": "728996", "filename": f"echo_2251_{timestamp}.csv", "path": pathToECHO2251},
        {"button_value": "728997", "filename": f"echo_2252_{timestamp}.csv", "path": pathToECHO2252}
    ]

    # Download files with specified options
    for file_info in files_info:
        download_file(file_info["path"], file_info["button_value"], file_info["filename"])


