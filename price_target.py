import os
import sys
import mysql.connector
from benzinga import financial_data

# Retrieve API key from environment variables
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

bz = financial_data.Benzinga(token)

# List of S&P 500 tickers (as provided)
sp500_tickers = [
    'MRNA'
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
                # Handle duplicate entry error and continue with the next record
                print(f"Duplicate entry found: {dup_err}. Continuing to next rating.")

        return added_ratings

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return added_ratings


def fetch_and_store_ratings(tickers, batch_size=50):
    tickers_managed = 0
    total_ratings_found = 0
    total_ratings_added = 0

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            tickers_managed += len(batch)

            params = {'company_tickers': ','.join(batch)}
            rating = bz.ratings(**params)
            ratings_found = len(rating.get("ratings", []))
            total_ratings_found += ratings_found

            if ratings_found > 0:
                ratings_added = insert_rating_data(rating, cursor)
                total_ratings_added += ratings_added

            print(f"Processed batch: {batch}. Ratings found: {ratings_found}. Ratings added: {ratings_added}.")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

    # Final logging
    print(f"Tickers managed: {tickers_managed}")
    print(f"Total ratings found: {total_ratings_found}")
    print(f"Total ratings added to database: {total_ratings_added}")

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
