import os
import sys
import mysql.connector
from benzinga import financial_data

# Retrieve API key from environment variables
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")

bz = financial_data.Benzinga(token)

# List of S&P 500 tickers
sp500_tickers = [
    'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE',
    # ... (other tickers)
    'WST', 'WDC', 'WY', 'WMB', 'WTW', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZTS'
]

# Database connection
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': 'db-mysql-nyc3-03005-do-user-4526552-0.h.db.ondigitalocean.com',
    'database': 'defaultdb',
    'port': 25060
}

def insert_rating_data(rating_data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        add_rating = ("INSERT INTO ratings "
                      "(id, action_company, action_pt, adjusted_pt_current, adjusted_pt_prior, analyst, analyst_name, "
                      "currency, date, exchange, importance, name, notes, pt_current, pt_prior, rating_current, "
                      "rating_prior, ticker, time, updated, url, url_calendar, url_news) "
                      "VALUES (%(id)s, %(action_company)s, %(action_pt)s, %(adjusted_pt_current)s, %(adjusted_pt_prior)s, "
                      "%(analyst)s, %(analyst_name)s, %(currency)s, %(date)s, %(exchange)s, %(importance)s, %(name)s, "
                      "%(notes)s, %(pt_current)s, %(pt_prior)s, %(rating_current)s, %(rating_prior)s, %(ticker)s, "
                      "%(time)s, %(updated)s, %(url)s, %(url_calendar)s, %(url_news)s)")

        for rating in rating_data["ratings"]:
            # Ensure correct data types
            rating["adjusted_pt_current"] = float(rating["adjusted_pt_current"]) if rating["adjusted_pt_current"] else None
            rating["adjusted_pt_prior"] = float(rating["adjusted_pt_prior"]) if rating["adjusted_pt_prior"]) else None
            rating["pt_current"] = float(rating["pt_current"]) if rating["pt_current"] else None
            rating["pt_prior"] = float(rating["pt_prior"]) if rating["pt_prior"] else None

            cursor.execute(add_rating, rating)

        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def fetch_and_store_ratings(tickers, batch_size=50):
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        params = {'company_tickers': ','.join(batch)}

        rating = bz.ratings(**params)
        print(bz.output(rating))
        insert_rating_data(rating)

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_ratings(sp500_tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
