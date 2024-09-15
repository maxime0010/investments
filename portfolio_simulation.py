import os
import mysql.connector
from datetime import datetime, timedelta
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

# Generate a list of dates from January 17, 2021, to today with one-week intervals
START_DATE = datetime(2021, 1, 17).date()  # Ensure START_DATE is a date
END_DATE = datetime.now().date()  # Ensure END_DATE is a date, stripping time

def get_existing_latest_record(cursor):
    """Fetch the latest record date from the portfolio_simulation table."""
    query = "SELECT MAX(date) FROM portfolio_simulation"
    cursor.execute(query)
    latest_record = cursor.fetchone()[0]
    print(f"[DEBUG] Latest record in the database: {latest_record}")
    return latest_record

def generate_date_list(start_date, end_date):
    """Generate a list of weekly dates from the start date to the end date."""
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(weeks=1)
    print(f"[DEBUG] Generated date list: {date_list}")
    return date_list

def get_closing_prices_as_of(cursor, date):
    """Fetch the closing prices for all stocks as of a given date."""
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
    prices = cursor.fetchall()
    print(f"[DEBUG] Closing prices as of {date}: {prices}")
    return prices

def fetch_portfolio_for_date(cursor, date):
    """Fetch the top 10 stocks to simulate the portfolio for a given date."""
    query = """
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis_simulation
        WHERE date = %s 
        AND num_combined_criteria >= %s
        AND stddev_combined_criteria <= 100  -- Standard deviation criterion
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """
    cursor.execute(query, (date, MIN_ANALYSTS))
    portfolio = cursor.fetchall()
    print(f"[DEBUG] Fetched portfolio for {date}: {portfolio}")
    return portfolio

def calculate_portfolio_value(cursor, date, previous_portfolio, closing_prices):
    """Calculate the value of the portfolio based on the previous week's portfolio and new closing prices."""
    closing_price_dict = {ticker: price for ticker, price in closing_prices}
    total_value = 0
    portfolio_value = []

    for ticker, last_closing_price, quantity, _ in previous_portfolio:
        last_closing_price = closing_price_dict.get(ticker, Decimal(0))
        if last_closing_price > 0:
            total_value_current = Decimal(quantity) * last_closing_price
            portfolio_value.append((ticker, last_closing_price, quantity, total_value_current))
            total_value += total_value_current
    print(f"[DEBUG] Portfolio value for {date}: {portfolio_value}, Total: {total_value}")
    return total_value, portfolio_value

def batch_insert_portfolio_simulation(cursor, portfolio_data):
    """Insert the simulated portfolio data into the portfolio_simulation table."""
    query = """
        INSERT INTO portfolio_simulation (date, ranking, ticker, stock_price, quantity, total_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, portfolio_data)
    print(f"[DEBUG] Inserted portfolio data: {portfolio_data}")

def simulate_portfolio(retries=3):
    """Main function to simulate the portfolio process."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Check if the table is empty
        latest_record = get_existing_latest_record(cursor)

        if not latest_record:
            # If table is empty, simulate from START_DATE
            print("[DEBUG] The table is empty. Starting from the beginning.")
            date_list = generate_date_list(START_DATE, END_DATE)
        else:
            # If latest_record is already a date, no conversion is needed
            if isinstance(latest_record, datetime):
                latest_record = latest_record.date()  # Convert datetime to date if needed

            print(f"[DEBUG] The table has records. Starting from {latest_record + timedelta(weeks=1)}.")
            start_date = latest_record + timedelta(weeks=1)
            date_list = generate_date_list(start_date, END_DATE)
        
        if not date_list:
            print("[DEBUG] No new dates to process.")
            return

        print(f"[DEBUG] Processing dates: {date_list}")

        # Initialize the first portfolio value (100 total, 10 per stock)
        initial_date = date_list[0]
        print(f"[DEBUG] Initial date for portfolio: {initial_date}")
        initial_portfolio = fetch_portfolio_for_date(cursor, initial_date)
        if not initial_portfolio:
            print(f"[DEBUG] No portfolio data available for {initial_date}")
            return

        closing_prices = get_closing_prices_as_of(cursor, initial_date)
        
        total_portfolio_value = Decimal(100)
        equal_value_per_stock = total_portfolio_value / Decimal(10)
        portfolio_value = []

        # Allocate initial value to each stock
        for row in initial_portfolio:
            ticker, expected_return, last_closing_price = row[:3]
            quantity = equal_value_per_stock / last_closing_price
            portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
        
        print(f"[DEBUG] Initial portfolio for {initial_date}: {portfolio_value}")
        
        # Prepare data for the first batch insert
        portfolio_data = [(initial_date, ranking + 1, ticker, stock_price, quantity, total_value)
                          for ranking, (ticker, stock_price, quantity, total_value) in enumerate(portfolio_value)]
        batch_insert_portfolio_simulation(cursor, portfolio_data)

        # Process for each subsequent week
        for date in date_list[1:]:
            for attempt in range(retries):
                try:
                    print(f"[DEBUG] Processing date: {date}")

                    # Fetch closing prices as of this date
                    closing_prices = get_closing_prices_as_of(cursor, date)
                    
                    # Calculate the portfolio value based on the previous week
                    total_portfolio_value, portfolio_value = calculate_portfolio_value(cursor, date, portfolio_value, closing_prices)
                    
                    # Fetch the new portfolio and rebalance
                    new_portfolio = fetch_portfolio_for_date(cursor, date)
                    
                    if new_portfolio:
                        equal_value_per_stock = total_portfolio_value / Decimal(10)
                        new_portfolio_value = []
                        
                        for row in new_portfolio:
                            ticker, expected_return, last_closing_price = row[:3]
                            quantity = equal_value_per_stock / last_closing_price
                            new_portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
                        
                        # Prepare batch insert data
                        portfolio_data = [(date, ranking + 1, ticker, stock_price, quantity, total_value)
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
