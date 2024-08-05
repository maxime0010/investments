import os

from benzinga import financial_data

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("BENZINGA_API_KEY")
if not api_key:
    raise ValueError("No API key found in environment variables.")

fin = financial_data.Benzinga(api_key)

stock_ratings = fin.ratings()

fin.output(stock_ratings)
