import os
import requests
import benzinga

from benzinga import financial_data

api_key = os.getenv("BENZINGA_API_KEY")
if not api_key:
    raise ValueError("No API key found in environment variables.")

fin = financial_data.Benzinga(api_key)

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# Define the parameters for the API call
parameters = {
    company_tickers = "AMZN"
}

fin.ratings(parameters)

