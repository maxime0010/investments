import os
import mysql.connector
from datetime import datetime, timedelta
from decimal import Decimal
from config import MIN_ANALYSTS
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

# Generate a list of dates from January 17, 2021, to today with one-week intervals
START_DATE = datetime(2021, 1, 17)
END_DATE = datetime.now()
date_list = []
current_date = START_DATE
while current_date <= END_DATE:
    date_list.append(current_date.strftime('%Y-%m-%d'))
    current_date += timedelta(weeks=1)

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
    for ticker, last_closing_price, quantity, _ in previous_portfolio:  # Adjusted to unpack 4 values
        last_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if last_closing_price > 0:
            total_value_current = Decimal(quantity) * last_closing_price
            portfolio_value.append((ticker, last_closing_price, quantity, total_value_current))
            total_value += total_value_current
    print(f"[DEBUG] Calculated portfolio value: {portfolio_value}")
    return total_value, portfolio_value

def batch_insert_portfolio_simulation(cursor, portfolio_data):
    query = """
        INSERT INTO portfolio_simulation (date, ranking, ticker, stock_price, quantity, total_value, total_portfolio_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    print(f"[DEBUG] Inserting portfolio simulation data: {portfolio_data}")
    cursor.executemany(query, portfolio_data)
    print(f"[DEBUG] Insert completed")

def simulate_portfolio(retries=3):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Initialize the first portfolio value (100 total, 10 per stock)
        initial_date = date_list[0]
        initial_portfolio = fetch_portfolio_for_date(cursor, initial_date)
        closing_prices = get_closing_prices_as_of(cursor, initial_date)
        
        total_portfolio_value = Decimal(100)
        equal_value_per_stock = total_portfolio_value / Decimal(10)
        portfolio_value = []

        # For the first portfolio, we allocate 10 units to each stock
        print(f"[DEBUG] Initial portfolio: {initial_portfolio}")
        for row in initial_portfolio:
            ticker, expected_return, last_closing_price = row[:3]
            print(f"[DEBUG] Processing stock: {ticker}, expected_return: {expected_return}, last_closing_price: {last_closing_price}")
            quantity = equal_value_per_stock / last_closing_price
            portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
        
        # Prepare data for the first batch insert
        portfolio_data = [(initial_date, ranking + 1, ticker, stock_price, quantity, total_value, total_portfolio_value)
                          for ranking, (ticker, stock_price, quantity, total_value) in enumerate(portfolio_value)]
        batch_insert_portfolio_simulation(cursor, portfolio_data)

        # Process for each subsequent date
        for date in date_list[1:]:
            for attempt in range(retries):
                try:
                    # Fetch closing prices as of this date
                    closing_prices = get_closing_prices_as_of(cursor, date)
                    
                    # Calculate the portfolio value based on the previous week
                    total_portfolio_value, portfolio_value = calculate_portfolio_value(cursor, date, portfolio_value, closing_prices)
                    
                    # Fetch the new portfolio and rebalance
                    new_portfolio = fetch_portfolio_for_date(cursor, date)
                    
                    if new_portfolio:
                        equal_value_per_stock = total_portfolio_value / Decimal(10)
                        new_portfolio_value = []
                        
                        print(f"[DEBUG] New portfolio for {date}: {new_portfolio}")
                        for row in new_portfolio:
                            ticker, expected_return, last_closing_price = row[:3]
                            print(f"[DEBUG] Processing stock: {ticker}, expected_return: {expected_return}, last_closing_price: {last_closing_price}")
                            quantity = equal_value_per_stock / last_closing_price
                            new_portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
                        
                        # Prepare batch insert data
                        portfolio_data = [(date, ranking + 1, ticker, stock_price, quantity, total_value, total_portfolio_value)
                                          for ranking, (ticker, stock_price, quantity, total_value) in enumerate(new_portfolio_value)]
                        batch_insert_portfolio_simulation(cursor, portfolio_data)

                    conn.commit()
                    break  # Break the retry loop if successful

                except mysql.connector.Error as err:
                    if err.errno == 1213:  # Deadlock error
                        print(f"Deadlock detected on {date}. Retrying... attempt {attempt + 1}")
                        conn.rollback()  # Rollback the transaction
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise  # Re-raise other MySQL errors

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
