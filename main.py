import os

from benzinga import financial_data

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("BENZINGA_API_KEY")
if not api_key:
    raise ValueError("No API key found in environment variables.")

client = benzinga.Client(api_key=api_key)

# Define the parameters for the API call
ticker = 'AMZN'
parameters = {
    'ticker': ticker,
    'fields': ['rating']
}

# Make the API call to get the rating for Amazon
response = client.get('/stock/research', params=parameters)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    # Extract the rating information from the response
    rating_info = data.get('rating', 'Rating not found')
    print(f"Rating for {ticker}: {rating_info}")
else:
    print(f"Failed to fetch rating for {ticker}. Status code: {response.status_code}")
