import os
from benzinga import financial_data
from datetime import datetime, timedelta

api_key = "9f4b718a05a44b2ba4c48a5fff692647"

# api_key = os.getenv("API_BENZINGA")
# if not api_key:
#     raise ValueError("No API key found in environment variables.")

fin = financial_data.Benzinga(api_key)

# Define the date range for the last 2 months
date_to = datetime.now().strftime("%Y-%m-%d")
date_from = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

# Fetch ratings data for Amazon
amazon_ratings = fin.ratings(company_tickers="AMZN", date_from=date_from, date_to=date_to)

# Print the output for verification
fin.output(amazon_ratings)

# Extract and display price targets from the ratings data
price_targets = [
    {
        "date": rating["date"],
        "analyst": rating["analyst_name"],
        "current_price_target": rating["pt_current"],
        "prior_price_target": rating.get("pt_prior", "N/A")
    }
    for rating in amazon_ratings["results"]
]

for target in price_targets:
    print(f"Date: {target['date']}, Analyst: {target['analyst']}, Current PT: {target['current_price_target']}, Prior PT: {target['prior_price_target']}")
