import os
import mysql.connector
from datetime import datetime
from config import DAYS_RECENT, SUCCESS_RATE_THRESHOLD, MIN_ANALYSTS

# Retrieve MySQL password from environment variables
mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")

# Database connection configuration
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': 'db-mysql-nyc3-03005-do-user-4526552-0.h.db.ondigitalocean.com',
    'database': 'defaultdb',
    'port': 25060
}

def update_portfolio_table(cursor):
    # Fetch the latest date from the portfolio table
    cursor.execute("SELECT MAX(date) FROM portfolio")
    latest_date = cursor.fetchone()[0]

    if not latest_date:
        print("No portfolio data available.")
        return

    # Calculate the current total value of the portfolio based on the latest stock prices
    cursor.execute("""
        SELECT p.ticker, p.quantity, a.last_closing_price
        FROM portfolio p
        JOIN analysis a ON p.ticker = a.ticker
        WHERE p.date = %s
    """, (latest_date,))
    portfolio_entries = cursor.fetchall()

    total_portfolio_value = sum(entry[1] * entry[2] for entry in portfolio_entries)

    if not total_portfolio_value:
        print("No value available for reinvestment.")
        return

    # Get the existing tickers in the portfolio for the latest date
    existing_tickers = set(entry[0] for entry in portfolio_entries)

    # Select the top 10 tickers by expected_return_combined_criteria from the analysis table
    cursor.execute(f"""
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis
        WHERE num_combined_criteria >= {MIN_ANALYSTS}
        ORDER BY expected_return_combined_criteria DESC
        LIMIT 10
    """)
    top_tickers = cursor.fetchall()

    # Check if there's a change in the portfolio
    new_tickers = set(ticker for ticker, _, _ in top_tickers)
    if existing_tickers != new_tickers:
        # Rebalance the portfolio by selling everything and reallocating the total value
        portfolio_data = []
        investment_per_stock = total_portfolio_value / 10  # Divide the total value among the 10 stocks

        for ranking, (ticker, expected_return_combined_criteria, last_price) in enumerate(top_tickers):
            if last_price and last_price > 0:
                quantity = investment_per_stock / last_price  # Calculate the number of shares to buy
                total_value = quantity * last_price  # Calculate the total value of the investment in this stock
            else:
                quantity = 0
                total_value = 0

            portfolio_data.append((latest_date, ranking + 1, ticker, quantity, total_value))

        # Clear previous entries for the current date
        cursor.execute("DELETE FROM portfolio WHERE date = %s", (latest_date,))
        cursor.executemany("""
            INSERT INTO portfolio (date, ranking, ticker, quantity, total_value)
            VALUES (%s, %s, %s, %s, %s)
        """, portfolio_data)

# Script execution
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()


    # Update the portfolio table with the top 10 tickers
    update_portfolio_table(cursor)

    conn.commit()
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
