import os
import sys
import mysql.connector
from datetime import datetime, timedelta
from benzinga import financial_data

# Retrieve API key from environment variables
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

bz = financial_data.Benzinga(token)

# List of S&P 500 tickers (as provided)
sp500_tickers = [
    'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD',
    # ... (rest of your tickers)
]

# Retrieve MySQL password from environment variables
mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")
host = os.getenv("MYSQL_HOST")
if not host:
    raise ValueError("No Host found in environment variables")

# Database connection configuration
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': host,
    'database': 'defaultdb',
    'port': 25060
}

def get_oldest_and_newest_rating_dates(cursor, ticker):
    query = """
        SELECT MIN(date) AS oldest_date, MAX(date) AS newest_date
        FROM ratings
        WHERE ticker = %s
    """
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    return result['oldest_date'], result['newest_date']

def insert_rating_data(rating_data, cursor):
    added_ratings = 0
    try:
        add_rating = ("INSERT INTO ratings "
                      "(id, action_company, action_pt, adjusted_pt_current, adjusted_pt_prior, analyst, analyst_name, "
                      "currency, date, exchange, importance, name, notes, pt_current, pt_prior, rating_current, "
                      "rating_prior, ticker, time, updated, url, url_calendar, url_news) "
                      "VALUES (%(id)s, %(action_company)s, %(action_pt)s, %(adjusted_pt_current)s, %(adjusted_pt_prior)s, "
                      "%(analyst)s, %(analyst_name)s, %(currency)s, %(date)s, %(exchange)s, %(importance)s, %(name)s, "
                      "%(notes)s, %(pt_current)s, %(pt_prior)s, %(rating_current)s, %(rating_prior)s, %(ticker)s, "
                      "%(time)s, %(updated)s, %(url)s, %(url_calendar)s, %(url_news)s) "
                      "ON DUPLICATE KEY UPDATE "
                      "action_company = VALUES(action_company), action_pt = VALUES(action_pt), "
                      "adjusted_pt_current = VALUES(adjusted_pt_current), adjusted_pt_prior = VALUES(adjusted_pt_prior), "
                      "analyst = VALUES(analyst), analyst_name = VALUES(analyst_name), currency = VALUES(currency), "
                      "date = VALUES(date), exchange = VALUES(exchange), importance = VALUES(importance), "
                      "name = VALUES(name), notes = VALUES(notes), pt_current = VALUES(pt_current), "
                      "pt_prior = VALUES(pt_prior), rating_current = VALUES(rating_current), "
                      "rating_prior = VALUES(rating_prior), ticker = VALUES(ticker), time = VALUES(time), "
                      "updated = VALUES(updated), url = VALUES(url), url_calendar = VALUES(url_calendar), "
                      "url_news = VALUES(url_news)")

        # Debugging: Print out the structure of rating_data
        print("Rating Data Structure:", type(rating_data))
        print("Rating Data Content:", rating_data)

        # Check if 'ratings' is a key in the response
        if isinstance(rating_data, dict) and "ratings" in rating_data:
            for rating in rating_data["ratings"]:
                # Ensure correct data types
                rating["adjusted_pt_current"] = float(rating["adjusted_pt_current"]) if rating["adjusted_pt_current"] else None
                rating["adjusted_pt_prior"] = float(rating["adjusted_pt_prior"]) if rating["adjusted_pt_prior"] else None
                rating["pt_current"] = float(rating["pt_current"]) if rating["pt_current"] else None
                rating["pt_prior"] = float(rating["pt_prior"]) if rating["pt_prior"] else None

                try:
                    cursor.execute(add_rating, rating)
                    added_ratings += 1
                except mysql.connector.IntegrityError as dup_err:
                    print(f"Duplicate entry found: {dup_err}. Continuing to next rating.")
        else:
            print("Unexpected response structure:", rating_data)

        return added_ratings

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return added_ratings

def fetch_and_store_ratings_for_ticker(ticker, cursor):
    today = datetime.today().date()
    five_years_ago = today - timedelta(days=5*365)

    oldest_date, newest_date = get_oldest_and_newest_rating_dates(cursor, ticker)

    if oldest_date and oldest_date >= five_years_ago:
        # Request all ratings between oldest_date and 5 years ago
        params = {
            'company_tickers': ticker,
            'date_from': (oldest_date - timedelta(days=1)).strftime('%Y-%m-%d'),
            'date_to': five_years_ago.strftime('%Y-%m-%d')
        }
        print(f"Fetching data for {ticker} from {params['date_from']} to {params['date_to']}")
        rating_data = bz.ratings(**params)
        insert_rating_data(rating_data, cursor)

    if newest_date and newest_date < today:
        # Request all ratings between newest_date and today
        params = {
            'company_tickers': ticker,
            'date_from': (newest_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            'date_to': today.strftime('%Y-%m-%d')
        }
        print(f"Fetching data for {ticker} from {params['date_from']} to {params['date_to']}")
        rating_data = bz.ratings(**params)
        insert_rating_data(rating_data, cursor)

def fetch_and_store_ratings(tickers):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        for ticker in tickers:
            fetch_and_store_ratings_for_ticker(ticker, cursor)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

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
