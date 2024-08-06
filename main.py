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


exit()
