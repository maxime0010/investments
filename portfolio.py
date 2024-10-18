import os
import mysql.connector
from datetime import datetime, timedelta
from decimal import Decimal
import time
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

def get_last_closing_price(cursor):
    """Fetch the most recent closing prices for all stocks."""
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
    closing_prices = cursor.fetchall()
    print(f"[DEBUG] Retrieved closing prices: {closing_prices}")
    return closing_prices

def update_existing_portfolio_simulation(cursor, today, closing_prices):
    """Update the most recent existing portfolio simulation with the latest closing prices."""
    closing_price_dict = {ticker: Decimal(price) for ticker, price in closing_prices}

    # Get the most recent portfolio to update
    cursor.execute("SELECT MAX(date) FROM portfolio_simulation WHERE date < %s", (today,))
    latest_portfolio_date = cursor.fetchone()[0]

    if not latest_portfolio_date:
        print("[DEBUG] No previous portfolio to update.")
        return  # No previous portfolio to update

    cursor.execute("SELECT ticker, quantity, total_value FROM portfolio_simulation WHERE date = %s", (latest_portfolio_date,))
    existing_portfolio = cursor.fetchall()

    print(f"[DEBUG] Retrieved existing portfolio for date {latest_portfolio_date}: {existing_portfolio}")

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

        quantity = Decimal(quantity)
        total_value_sell = quantity * latest_closing_price
        total_value = Decimal(total_value)
        evolution = total_value_sell - total_value

        print(f"[DEBUG] Updating {ticker}: Quantity={quantity}, Latest Price={latest_closing_price}, Sell Value={total_value_sell}, Evolution={evolution}")

        cursor.execute("""
            UPDATE portfolio_simulation
            SET date_sell = %s, 
                stock_price_sell = %s, 
                total_value_sell = %s, 
                evolution = %s
            WHERE ticker = %s AND date = %s
        """, (today, latest_closing_price, total_value_sell, evolution, ticker, latest_portfolio_date))

def fetch_new_portfolio(cursor):
    """Fetch the top 10 stocks from the latest available date in the analysis_simulation table."""
    
    # Step 1: Get the latest date from the analysis_simulation table
    cursor.execute("SELECT MAX(date) FROM analysis_simulation")
    latest_date = cursor.fetchone()[0]

    if latest_date is None:
        print("[DEBUG] No records found in analysis_simulation.")
        return []  # Return an empty list if no data is found

    print(f"[DEBUG] Latest date in analysis_simulation: {latest_date}")

    # Step 2: Fetch the top 10 stocks from the latest date
    cursor.execute("""
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis_simulation
        WHERE num_combined_criteria >= %s
        AND stddev_combined_criteria <= 100  -- New criterion: standard deviation <= 100
        AND date = %s
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """, (MIN_ANALYSTS, latest_date))

    new_portfolio = cursor.fetchall()
    print(f"[DEBUG] Fetched new portfolio: {new_portfolio}")
    return new_portfolio

def insert_new_portfolio_simulation(cursor, today, new_portfolio, closing_prices):
    """Insert the new portfolio simulation for the current date."""
    closing_price_dict = {ticker: Decimal(price) for ticker, price in closing_prices}

    # Get the total value from selling the previous portfolio
    cursor.execute("SELECT SUM(total_value_sell) FROM portfolio_simulation WHERE date_sell = %s", (today,))
    aggregated_total_value_sell = Decimal(cursor.fetchone()[0] or 0)
    equal_value_per_stock = aggregated_total_value_sell / Decimal(10)

    print(f"[DEBUG] Aggregated total value from sell: {aggregated_total_value_sell}, Equal value per stock: {equal_value_per_stock}")

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

        print(f"[DEBUG] Inserting {ticker}: Ranking={ranking}, Price={last_closing_price}, Quantity={quantity}, Total Value={total_value}")

        cursor.execute("""
            INSERT INTO portfolio_simulation (date, ranking, ticker, stock_price, quantity, total_value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (today, ranking, ticker, last_closing_price, quantity, total_value))

# Execution
try:
    # Check if today is Sunday (6 = Sunday in weekday() method)
    if datetime.today().weekday() == 4:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        today = datetime.today().date()
        closing_prices = get_last_closing_price(cursor)

        # Step 1: Update the most recent existing portfolio in portfolio_simulation
        update_existing_portfolio_simulation(cursor, today, closing_prices)

        # Step 2: Fetch the new portfolio based on analysis_simulation
        new_portfolio = fetch_new_portfolio(cursor)
        if new_portfolio:
            # Step 3: Insert the new portfolio into portfolio_simulation
            insert_new_portfolio_simulation(cursor, today, new_portfolio, closing_prices)

        # Commit the changes to the database
        conn.commit()

        cursor.close()
        conn.close()
    else:
        print(f"[DEBUG] Today is not Sunday. Portfolio update will not run. Today is {datetime.today().strftime('%A')}.")

except mysql.connector.Error as err:
    print(f"Database error: {err}")
    if conn:
        conn.rollback()
    cursor.close()
    conn.close()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    if conn:
        conn.rollback()
    cursor.close()
    conn.close()
