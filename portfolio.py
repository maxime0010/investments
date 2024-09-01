import os
import mysql.connector
from datetime import datetime
from decimal import Decimal
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
            CAST(close AS DECIMAL(10, 2)) AS last_closing_price
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
    cursor.execute("SELECT ranking, ticker, quantity, total_value FROM portfolio WHERE date = %s", (date,))
    return cursor.fetchall()

def update_existing_portfolio(cursor, today, closing_prices):
    closing_price_dict = {ticker: Decimal(price) for ticker, price in closing_prices}

    # Update the most recent existing portfolio
    cursor.execute("SELECT MAX(date) FROM portfolio WHERE date < %s", (today,))
    latest_portfolio_date = cursor.fetchone()[0]

    if not latest_portfolio_date:
        return  # No previous portfolio to update

    cursor.execute("SELECT ticker, quantity, total_value FROM portfolio WHERE date = %s", (latest_portfolio_date,))
    existing_portfolio = cursor.fetchall()

    for ticker, quantity, total_value in existing_portfolio:
        latest_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if latest_closing_price == 0:
            # Find the most recent non-zero closing price
            cursor.execute("""
                SELECT CAST(close AS DECIMAL(10, 2)) 
                FROM prices 
                WHERE ticker = %s AND close > 0 
                ORDER BY date DESC 
                LIMIT 1
            """, (ticker,))
            latest_closing_price = Decimal(cursor.fetchone()[0])

        quantity = Decimal(quantity)  # Ensure quantity is a Decimal
        total_value_sell = quantity * latest_closing_price
        total_value = Decimal(total_value)  # Ensure total_value is a Decimal
        evolution = total_value_sell - total_value

        cursor.execute("""
            UPDATE portfolio
            SET date_sell = %s, 
                stock_price_sell = %s, 
                total_value_sell = %s, 
                evolution = %s
            WHERE ticker = %s AND date = %s
        """, (today, latest_closing_price, total_value_sell, evolution, ticker, latest_portfolio_date))

def insert_new_portfolio(cursor, today, new_portfolio, closing_prices):
    closing_price_dict = {ticker: Decimal(price) for ticker, price in closing_prices}

    cursor.execute("SELECT SUM(total_value_sell) FROM portfolio WHERE date_sell = %s", (today,))
    aggregated_total_value_sell = Decimal(cursor.fetchone()[0] or 0)
    equal_value_per_stock = aggregated_total_value_sell / Decimal(10)

    for ranking, (ticker, expected_return, _) in enumerate(new_portfolio, start=1):
        last_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if last_closing_price == 0:
            # Find the most recent non-zero closing price
            cursor.execute("""
                SELECT CAST(close AS DECIMAL(10, 2)) 
                FROM prices 
                WHERE ticker = %s AND close > 0 
                ORDER BY date DESC 
                LIMIT 1
            """, (ticker,))
            last_closing_price = Decimal(cursor.fetchone()[0])

        total_value = equal_value_per_stock
        quantity = total_value / last_closing_price

        cursor.execute("""
            INSERT INTO portfolio (date, ranking, ticker, stock_price, quantity, total_value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (today, ranking, ticker, last_closing_price, quantity, total_value))

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

    today = datetime.today().date()
    closing_prices = get_last_closing_price(cursor)

    # Step 1: Update the most recent existing portfolio
    update_existing_portfolio(cursor, today, closing_prices)

    # Step 2: Create and insert the new portfolio
    new_portfolio = fetch_new_portfolio(cursor)
    insert_new_portfolio(cursor, today, new_portfolio, closing_prices)

    conn.commit()
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
