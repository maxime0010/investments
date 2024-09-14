import os
import mysql.connector
from datetime import datetime, timedelta
from decimal import Decimal
from config import MIN_ANALYSTS

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

# Generate a list of dates from September 1, 2019, to today with one-week intervals
START_DATE = datetime(2019, 9, 1)
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
    cursor.execute(query, (date,))
    return cursor.fetchall()

def fetch_portfolio_for_date(cursor, date):
    query = """
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis_simulation
        WHERE date = %s AND num_combined_criteria >= %s
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """
    cursor.execute(query, (date, MIN_ANALYSTS))
    return cursor.fetchall()

def calculate_portfolio_value(cursor, date, previous_portfolio, closing_prices):
    closing_price_dict = {ticker: price for ticker, price in closing_prices}
    total_value = 0
    portfolio_value = []
    
    for ticker, quantity, total_value in previous_portfolio:
        last_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if last_closing_price > 0:
            total_value = Decimal(quantity) * last_closing_price
        portfolio_value.append((ticker, last_closing_price, quantity, total_value))
        total_value += total_value
    
    return total_value, portfolio_value

def insert_portfolio_simulation(cursor, date, portfolio_value, total_portfolio_value):
    for ranking, (ticker, stock_price, quantity, total_value) in enumerate(portfolio_value, start=1):
        cursor.execute("""
            INSERT INTO portfolio_simulation (date, ranking, ticker, stock_price, quantity, total_value, total_portfolio_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (date, ranking, ticker, stock_price, quantity, total_value, total_portfolio_value))

# Execution
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Initialize with the first portfolio value (100 total, 10 per stock)
    initial_date = date_list[0]
    initial_portfolio = fetch_portfolio_for_date(cursor, initial_date)
    closing_prices = get_closing_prices_as_of(cursor, initial_date)
    
    # For the first portfolio, we allocate 10 units to each stock
    total_portfolio_value = Decimal(100)
    equal_value_per_stock = total_portfolio_value / Decimal(10)
    portfolio_value = []
    
    for ticker, expected_return, last_closing_price in initial_portfolio:
        quantity = equal_value_per_stock / last_closing_price
        portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
    
    # Insert the initial portfolio
    insert_portfolio_simulation(cursor, initial_date, portfolio_value, total_portfolio_value)

    # Process for each subsequent date
    for date in date_list[1:]:
        # Fetch closing prices as of this date
        closing_prices = get_closing_prices_as_of(cursor, date)
        
        # Calculate the portfolio value based on the previous week
        total_portfolio_value, portfolio_value = calculate_portfolio_value(cursor, date, portfolio_value, closing_prices)
        
        # Fetch the new portfolio and rebalance
        new_portfolio = fetch_portfolio_for_date(cursor, date)
        
        if new_portfolio:
            equal_value_per_stock = total_portfolio_value / Decimal(10)
            new_portfolio_value = []
            
            for ticker, expected_return, last_closing_price in new_portfolio:
                quantity = equal_value_per_stock / last_closing_price
                new_portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
            
            # Insert the new portfolio for this date
            insert_portfolio_simulation(cursor, date, new_portfolio_value, total_portfolio_value)
    
    conn.commit()
    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
