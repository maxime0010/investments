import os
import mysql.connector
from datetime import datetime
from config import DAYS_RECENT, SUCCESS_RATE_THRESHOLD, MIN_ANALYSTS

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

def get_latest_closing_date(cursor):
    cursor.execute("SELECT MAX(date) FROM prices")
    return cursor.fetchone()[0]

def get_last_closing_price(cursor):
    query = """
        SELECT 
            ticker,
            close AS last_closing_price
        FROM prices
        WHERE (ticker, date) IN (
            SELECT ticker, MAX(date) 
            FROM prices 
            GROUP BY ticker
        )
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_existing_portfolio(cursor, date):
    cursor.execute("SELECT ticker, quantity FROM portfolio WHERE date = %s", (date,))
    return cursor.fetchall()

def calculate_total_portfolio_value(portfolio, closing_prices):
    total_value = 0
    closing_price_dict = dict(closing_prices)
    for ticker, quantity in portfolio:
        total_value += quantity * closing_price_dict.get(ticker, 0)
    return total_value

def insert_or_update_portfolio(cursor, date, portfolio_data):
    for data in portfolio_data:
        # Ensure correct data order: (date, ranking, ticker, quantity, total_value)
        data_tuple = (
            date,                # First value: date
            data[0],             # Second value: ranking
            data[1],             # Third value: ticker
            data[2] if len(data) > 2 else None,  # Fourth value: quantity
            data[3] if len(data) > 3 else None   # Fifth value: total_value
        )
        print(f"Executing SQL with data: {data_tuple}")  # Debugging statement
        cursor.execute("""
            INSERT INTO portfolio (date, ranking, ticker, quantity, total_value)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE quantity = VALUES(quantity), total_value = VALUES(total_value)
        """, data_tuple)

def update_portfolio(cursor, latest_date, new_portfolio, closing_prices):
    existing_portfolio = get_existing_portfolio(cursor, latest_date)
    closing_price_dict = dict(closing_prices)

    if existing_portfolio:
        existing_tickers = set(ticker for ticker, _ in existing_portfolio)
        new_tickers = set(ticker for ticker, _, _ in new_portfolio)
        
        if existing_tickers == new_tickers:
            insert_or_update_portfolio(cursor, latest_date, new_portfolio)
        else:
            total_value = calculate_total_portfolio_value(existing_portfolio, closing_prices)
            equal_value_per_stock = total_value / len(new_portfolio)
            new_portfolio_data = []

            for ranking, (ticker, _, last_price) in enumerate(new_portfolio, start=1):
                quantity = equal_value_per_stock / last_price if last_price else 0
                total_value_stock = quantity * last_price
                new_portfolio_data.append((ranking, ticker, quantity, total_value_stock))
            
            cursor.execute("DELETE FROM portfolio WHERE date = %s", (latest_date,))
            insert_or_update_portfolio(cursor, latest_date, new_portfolio_data)
    else:
        new_portfolio_data = []
        for ranking, (ticker, _, last_price) in enumerate(new_portfolio, start=1):
            new_portfolio_data.append((ranking, ticker, None, None))
        insert_or_update_portfolio(cursor, latest_date, new_portfolio_data)

def fetch_new_portfolio(cursor):
    cursor.execute("""
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis
        WHERE num_combined_criteria >= %s
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """, (MIN_ANALYSTS,))
    return cursor.fetchall()

# Execution
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    latest_date = get_latest_closing_date(cursor)
    closing_prices = get_last_closing_price(cursor)
    new_portfolio = fetch_new_portfolio(cursor)

    update_portfolio(cursor, latest_date, new_portfolio, closing_prices)

    conn.commit()
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
