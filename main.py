import os

from benzinga import financial_data
from benzinga import news_data

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("BENZINGA_API_KEY")
fin = financial_data.Benzinga(api_key)



paper = news_data.News(api_key)

stories = paper.news()

paper.output(stories)
