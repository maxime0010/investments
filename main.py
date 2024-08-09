import os
import requests

# Retrieve Benzinga API token from environment variables
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API token found in environment variables")

def fetch_analysts_data():
    url = "https://api.benzinga.com/api/v2.1/calendar/ratings/analysts"
    querystring = {"token": token}
    response = requests.get(url, params=querystring)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

def main():
    analysts_data = fetch_analysts_data()
    if analysts_data:
        print(analysts_data)  # Display the output of the API request

if __name__ == "__main__":
    main()
