import os
import sys
import mysql.connector
from datetime import datetime
from benzinga import financial_data

# Retrieve API key and MySQL credentials from environment variables
token = os.getenv("BENZINGA_API_KEY")
mdp = os.getenv("MYSQL_MDP")
host = os.getenv("MYSQL_HOST")

if not token or not mdp or not host:
    raise ValueError("API key or MySQL credentials missing in environment variables")

# Initialize Benzinga API client
bz = financial_data.Benzinga(token)

# Database connection configuration
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': host,
    'database': 'defaultdb',
    'port': 25060
}

# List of tickers to fetch ratings for
tickers = ['MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD']

def safe_cast(value, target_type, default=None):
    """Safely cast values to the target type, with a fallback to a default value if casting fails."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default

def insert_rating_data(rating_data, cursor):
    """Insert rating data into the MySQL database."""
    try:
        add_rating = ("""
            INSERT INTO ratings 
            (id, action_company, action_pt, adjusted_pt_current, adjusted_pt_prior, analyst, analyst_name,
             currency, date, exchange, importance, name, notes, pt_current, pt_prior, rating_current, 
             rating_prior, ticker, time, updated, url, url_calendar, url_news) 
            VALUES (%(id)s, %(action_company)s, %(action_pt)s, %(adjusted_pt_current)s, %(adjusted_pt_prior)s, 
                    %(analyst)s, %(analyst_name)s, %(currency)s, %(date)s, %(exchange)s, %(importance)s, 
                    %(name)s, %(notes)s, %(pt_current)s, %(pt_prior)s, %(rating_current)s, %(rating_prior)s, 
                    %(ticker)s, %(time)s, %(updated)s, %(url)s, %(url_calendar)s, %(url_news)s)
            ON DUPLICATE KEY UPDATE
            adjusted_pt_current = VALUES(adjusted_pt_current), 
            adjusted_pt_prior = VALUES(adjusted_pt_prior), 
            pt_current = VALUES(pt_current), 
            pt_prior = VALUES(pt_prior), 
            rating_current = VALUES(rating_current),
            rating_prior = VALUES(rating_prior), 
            updated = VALUES(updated)
        """)

        added_ratings = 0
        for rating in rating_data:
            # Safely cast each value to the appropriate data type
            rating['adjusted_pt_current'] = safe_cast(rating.get('adjusted_pt_current'), float, None)
            rating['adjusted_pt_prior'] = safe_cast(rating.get('adjusted_pt_prior'), float, None)
            rating['pt_current'] = safe_cast(rating.get('pt_current'), float, None)
            rating['pt_prior'] = safe_cast(rating.get('pt_prior'), float, None)

            try:
                cursor.execute(add_rating, rating)
                added_ratings += 1
            except mysql.connector.Error as err:
                print(f"Error inserting rating data for {rating['ticker']} at {rating['date']}: {err}")
                continue  # Skip to the next rating if there's an issue with the current one

        return added_ratings

    except mysql.connector.Error as err:
        print(f"Error inserting rating data: {err}")
        return 0

def fetch_ratings_for_january_2021(ticker, cursor):
    """Fetch ratings for January 2021 for the given ticker."""
    params = {
        'company_tickers': ticker,
        'date_from': '2021-01-01',
        'date_to': '2021-01-31'
    }

    print(f"Fetching January 2021 ratings for {ticker}...")
    
    try:
        rating_data = bz.ratings(**params)
        if rating_data and 'ratings' in rating_data and rating_data['ratings']:
            added_ratings = insert_rating_data(rating_data['ratings'], cursor)
            print(f"Successfully added {added_ratings} ratings for {ticker}")
        else:
            print(f"No ratings found for {ticker} in January 2021")
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")

def fetch_and_store_ratings(tickers):
    """Fetch and store ratings for each ticker in January 2021."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        for ticker in tickers:
            fetch_ratings_for_january_2021(ticker, cursor)

        conn.commit()
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def exit_program():
    """Exit the program."""
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_ratings(tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
