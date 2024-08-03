import os

from benzinga import financial_data
from benzinga import news_data

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()




import requests

url = "https://api.benzinga.com/api/v1/analyst/insights"

querystring = {"token":"9f4b718a05a44b2ba4c48a5fff692647","ticker":"AMZN"}

response = requests.request("GET", url, params=querystring)

print(response.text)

