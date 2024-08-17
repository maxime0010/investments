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
    'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN', 
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

def insert_price_data(price_data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        add_price = ("INSERT INTO prices (ticker, date, close) "
                     "VALUES (%(ticker)s, %(date)s, %(close)s) "
                     "ON DUPLICATE KEY UPDATE close = VALUES(close)")

        for price in price_data:
            cursor.execute(add_price, price)
        
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def fetch_and_store_prices(tickers, batch_size=50):
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        params = {'company_tickers': ','.join(batch)}
        
        try:
            quotes = bz.delayed_quote(**params)
            if quotes:
                print(f"Fetched data for batch: {batch}")
                print(bz.output(quotes))
                price_data = []
                for quote in quotes['quotes']:
                    price_data.append({
                        'ticker': quote['security']['symbol'],
                        'date': quote['quote']['date'][:10],  # Extracting the date part from the datetime
                        'close': quote['quote']['last']
                    })
                insert_price_data(price_data)
            else:
                print(f"No data returned for batch: {batch}")
        except Exception as e:
            print(f"Error fetching prices for batch {batch}: {e}")

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_prices(sp500_tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
