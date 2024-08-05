import os
import requests
import benzinga

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("BENZINGA_API_KEY")
if not api_key:
    raise ValueError("No API key found in environment variables.")

base_url = 'https://api.benzinga.com/api/v2/analyst_ratings'

# Define the parameters for the API call
parameters = {
    'token': api_key,
    'symbols': 'AMZN'
}

# Make the API call to get the rating for Amazon
response = requests.get(base_url, params=parameters)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    # Extract the rating information from the response
    if 'analysts' in data and data['analysts']:
        rating_info = data['analysts'][0]  # Assuming the first analyst rating is desired
        print(f"Rating for AMZN: {rating_info}")
    else:
        print("Rating not found for AMZN.")
else:
    print(f"Failed to fetch rating for AMZN. Status code: {response.status_code}")
    print(f"Response: {response.text}")
