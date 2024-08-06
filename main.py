import os
import requests
import benzinga

token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

from benzinga import financial_data

bz = financial_data.Benzinga(token)
price = bz.delayed_quote(company_tickers = "AAPL") 
print(bz.output(price))

from benzinga import news_data

bz_news = news_data.News(token)
news = bz_news.news(company_tickers = "AAPL", 
                    base_date = "2019-10-03")
print(bz_news.output(news[-1])) #returns the most recent news item.

exit()
