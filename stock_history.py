import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# List of tickers
tickers = [
    'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 
    # ... (rest of your tickers)
]

# Set up the download directory and Chrome options
download_dir = "historical_data"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

chrome_options = Options()
prefs = {"download.default_directory": os.path.abspath(download_dir)}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")  # Run in headless mode (no browser window)

# Set up the WebDriver (ensure chromedriver is in your PATH or provide the path here)
driver = webdriver.Chrome(service=Service("/path/to/chromedriver"), options=chrome_options)

# Iterate over each ticker and download the historical data
for ticker in tickers:
    try:
        url = f"https://www.nasdaq.com/market-activity/stocks/{ticker.lower()}/historical?page=1&rows_per_page=10&timeline=y10"
        driver.get(url)

        # Allow the page to load
        time.sleep(5)  # Adjust as necessary based on your connection speed

        # Click on the "Download historical data" button
        download_button = driver.find_element(By.XPATH, "//a[contains(text(), 'Download Data')]")
        download_button.click()

        # Wait for the download to complete
        time.sleep(5)  # Adjust as necessary based on file size

        # Rename the downloaded file
        downloaded_file = os.path.join(download_dir, "historical.csv")
        renamed_file = os.path.join(download_dir, f"{ticker}.csv")
        if os.path.exists(downloaded_file):
            os.rename(downloaded_file, renamed_file)
            print(f"Downloaded and renamed {ticker}.csv")
        else:
            print(f"Failed to download data for {ticker}")

    except Exception as e:
        print(f"An error occurred for {ticker}: {e}")

# Clean up
driver.quit()
