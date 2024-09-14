import os
import mysql.connector
from datetime import datetime
from decimal import Decimal
from config import DAYS_RECENT, SUCCESS_RATE_THRESHOLD, MIN_ANALYSTS
import time

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

def get_closing_prices_as_of(cursor, date):
    query = """
        SELECT 
            ticker,
            CAST(close AS DECIMAL(10, 2)) AS closing_price
        FROM prices
        WHERE (ticker, date) IN (
            SELECT ticker, MAX(date) 
            FROM prices 
            WHERE date <= %s
            GROUP BY ticker
        )
    """
    print(f"[DEBUG] Fetching closing prices for {date}")
    cursor.execute(query, (date,))
    result = cursor.fetchall()
    print(f"[DEBUG] Retrieved closing prices: {result}")
    return result

def fetch_portfolio_for_date(cursor, date):
    """Fetch the portfolio for the given date."""
    query = """
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis_simulation
        WHERE date = %s AND num_combined_criteria >= %s
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """
    print(f"[DEBUG] Fetching portfolio for {date}")
    cursor.execute(query, (date, MIN_ANALYSTS))
    result = cursor.fetchall()
    print(f"[DEBUG] Retrieved portfolio: {result}")
    return result

def calculate_portfolio_value(cursor, date, previous_portfolio, closing_prices):
    closing_price_dict = {ticker: price for ticker, price in closing_prices}
    total_value = 0
    portfolio_value = []
    
    print(f"[DEBUG] Calculating portfolio value for {date}")
    for ticker, last_closing_price, quantity, _ in previous_portfolio:
        last_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if last_closing_price > 0:
            total_value_current = Decimal(quantity) * last_closing_price
            portfolio_value.append((ticker, last_closing_price, quantity, total_value_current))
            total_value += total_value_current
    print(f"[DEBUG] Calculated portfolio value: {portfolio_value}")
    return total_value, portfolio_value

def update_existing_portfolio(cursor, today, closing_prices):
    closing_price_dict = {ticker: Decimal(price) for ticker, price in closing_prices}

    # Update the most recent existing portfolio
    cursor.execute("SELECT MAX(date) FROM portfolio_simulation WHERE date < %s", (today,))
    latest_portfolio_date = cursor.fetchone()[0]

    if not latest_portfolio_date:
        return  # No previous portfolio to update

    cursor.execute("SELECT ticker, quantity, total_value FROM portfolio_simulation WHERE date = %s", (latest_portfolio_date,))
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

        quantity = Decimal(quantity)
        total_value_sell = quantity * latest_closing_price
        total_value = Decimal(total_value)
        evolution = total_value_sell - total_value

        cursor.execute("""
            UPDATE portfolio_simulation
            SET date_sell = %s, 
                stock_price_sell = %s, 
                total_value_sell = %s, 
                evolution = %s
            WHERE ticker = %s AND date = %s
        """, (today, latest_closing_price, total_value_sell, evolution, ticker, latest_portfolio_date))

def batch_insert_portfolio_simulation(cursor, portfolio_data):
    query = """
        INSERT INTO portfolio_simulation (date, ranking, ticker, stock_price, quantity, total_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    print(f"[DEBUG] Inserting portfolio simulation data: {portfolio_data}")
    cursor.executemany(query, portfolio_data)
    print(f"[DEBUG] Insert completed")

def simulate_portfolio(retries=3):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Step 1: Retrieve all unique dates from the analysis_simulation table
        cursor.execute("SELECT DISTINCT date FROM analysis_simulation ORDER BY date ASC")
        analysis_dates = cursor.fetchall()

        if not analysis_dates:
            print("[DEBUG] No dates found in analysis_simulation.")
            return

        # Initialize the first portfolio value (100 total, 10 per stock)
        total_portfolio_value = Decimal(100)

        for row in analysis_dates:
            date = row['date'].strftime('%Y-%m-%d')  # Convert to string for processing
            print(f"[DEBUG] Processing date: {date}")

            # Fetch closing prices and portfolio for the current date
            closing_prices = get_closing_prices_as_of(cursor, date)
            new_portfolio = fetch_portfolio_for_date(cursor, date)

            if new_portfolio:
                equal_value_per_stock = total_portfolio_value / Decimal(10)
                portfolio_value = []
                
                print(f"[DEBUG] New portfolio for {date}: {new_portfolio}")
                for row in new_portfolio:
                    ticker, expected_return, last_closing_price = row[:3]
                    quantity = equal_value_per_stock / last_closing_price
                    portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))

                # Prepare batch insert data
                portfolio_data = [(date, ranking + 1, ticker, last_closing_price, quantity, equal_value_per_stock)
                                  for ranking, (ticker, last_closing_price, quantity, _) in enumerate(portfolio_value)]
                batch_insert_portfolio_simulation(cursor, portfolio_data)

            # Update the previous portfolio with sell details
            update_existing_portfolio(cursor, date, closing_prices)

            # Commit the transaction
            conn.commit()

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        if conn:
            conn.rollback()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        cursor.close()
        conn.close()

# Run the simulation
simulate_portfolio()
