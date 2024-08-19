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

def calculate_price_target_statistics(cursor):
    query = f"""
        SELECT 
            r.ticker,
            AVG(r.adjusted_pt_current) AS average_price_target,
            STDDEV(r.adjusted_pt_current) AS stddev_price_target,
            COUNT(DISTINCT r.analyst_name) AS num_analysts,
            MAX(r.date) AS last_update_date,
            AVG(DATEDIFF(NOW(), r.date)) AS avg_days_since_last_update,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) THEN r.analyst_name END) AS num_recent_analysts,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) THEN r.adjusted_pt_current END) AS stddev_price_target_recent,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) THEN r.adjusted_pt_current END) AS average_price_target_recent,
            COUNT(DISTINCT CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.analyst_name END) AS num_high_success_analysts,
            STDDEV(CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS stddev_high_success_analysts,
            AVG(CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS avg_high_success_analysts,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.analyst_name END) AS num_combined_criteria,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS stddev_combined_criteria,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS avg_combined_criteria
        FROM (
            SELECT 
                ticker,
                analyst_name,
                MAX(date) AS latest_date
            FROM ratings
            GROUP BY ticker, analyst_name
        ) AS latest_ratings
        JOIN ratings AS r 
        ON latest_ratings.ticker = r.ticker 
        AND latest_ratings.analyst_name = r.analyst_name 
        AND latest_ratings.latest_date = r.date
        JOIN analysts AS a 
        ON r.analyst_name = a.name_full
        GROUP BY r.ticker
    """
    cursor.execute(query)
    return cursor.fetchall()

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

def calculate_and_insert_analysis(cursor, target_statistics, closing_prices):
    analysis_data = []
    closing_price_dict = {price[0]: price[1] for price in closing_prices}
    
    for stats in target_statistics:
        ticker = stats[0]
        average_price_target = stats[1]
        stddev_price_target = stats[2]
        num_analysts = stats[3]
        last_update_date = stats[4]
        avg_days_since_last_update = stats[5]
        num_recent_analysts = stats[6]
        stddev_price_target_recent = stats[7]
        average_price_target_recent = stats[8]
        num_high_success_analysts = stats[9]
        stddev_high_success_analysts = stats[10]
        avg_high_success_analysts = stats[11]
        num_combined_criteria = stats[12]
        stddev_combined_criteria = stats[13]
        avg_combined_criteria = stats[14]
        
        last_closing_price = closing_price_dict.get(ticker)
        
        if last_closing_price is not None and average_price_target is not None:
            expected_return = ((average_price_target - last_closing_price) / last_closing_price) * 100
            days_since_last_update = (datetime.now().date() - last_update_date).days
            
            expected_return_recent = None
            if average_price_target_recent is not None:
                expected_return_recent = ((average_price_target_recent - last_closing_price) / last_closing_price) * 100

            expected_return_high_success = None
            if avg_high_success_analysts is not None:
                expected_return_high_success = ((avg_high_success_analysts - last_closing_price) / last_closing_price) * 100
            
            expected_return_combined_criteria = None
            if avg_combined_criteria is not None:
                expected_return_combined_criteria = ((avg_combined_criteria - last_closing_price) / last_closing_price) * 100

            analysis_data.append((ticker, last_closing_price, average_price_target, expected_return, 
                                  num_analysts, stddev_price_target, days_since_last_update, avg_days_since_last_update,
                                  num_recent_analysts, stddev_price_target_recent, expected_return_recent,
                                  num_high_success_analysts, stddev_high_success_analysts, avg_high_success_analysts, expected_return_high_success,
                                  num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria))

    insert_query = """
        INSERT INTO analysis (ticker, last_closing_price, average_price_target, expected_return, num_analysts, stddev_price_target, days_since_last_update, avg_days_since_last_update, num_recent_analysts, stddev_price_target_recent, expected_return_recent, num_high_success_analysts, stddev_high_success_analysts, avg_high_success_analysts, expected_return_high_success, num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            last_closing_price = VALUES(last_closing_price), 
            average_price_target = VALUES(average_price_target), 
            expected_return = VALUES(expected_return),
            num_analysts = VALUES(num_analysts),
            stddev_price_target = VALUES(stddev_price_target),
            days_since_last_update = VALUES(days_since_last_update),
            avg_days_since_last_update = VALUES(avg_days_since_last_update),
            num_recent_analysts = VALUES(num_recent_analysts),
            stddev_price_target_recent = VALUES(stddev_price_target_recent),
            expected_return_recent = VALUES(expected_return_recent),
            num_high_success_analysts = VALUES(num_high_success_analysts),
            stddev_high_success_analysts = VALUES(stddev_high_success_analysts),
            avg_high_success_analysts = VALUES(avg_high_success_analysts),
            expected_return_high_success = VALUES(expected_return_high_success),
            num_combined_criteria = VALUES(num_combined_criteria),
            stddev_combined_criteria = VALUES(stddev_combined_criteria),
            avg_combined_criteria = VALUES(avg_combined_criteria),
            expected_return_combined_criteria = VALUES(expected_return_combined_criteria)
    """
    cursor.executemany(insert_query, analysis_data)

def update_portfolio_table(cursor):
    # Fetch the latest date from the prices table
    cursor.execute("SELECT MAX(date) FROM prices")
    latest_date = cursor.fetchone()[0]

    if not latest_date:
        print("No price data available.")
        return

    # Calculate the total value of the current portfolio
    cursor.execute("SELECT SUM(total_value) FROM portfolio WHERE date = %s", (latest_date,))
    total_portfolio_value = cursor.fetchone()[0] or 100  # Default to 100 if the portfolio is empty

    # Get the existing tickers in the portfolio for the latest date
    cursor.execute("SELECT ticker FROM portfolio WHERE date = %s", (latest_date,))
    existing_tickers = set(row[0] for row in cursor.fetchall())

    # Select the top 10 tickers by expected return from the analysis table
    cursor.execute("""
        SELECT ticker, expected_return, last_closing_price
        FROM analysis
        ORDER BY expected_return DESC
        LIMIT 10
    """)
    top_tickers = cursor.fetchall()

    # Check if there's a change in the portfolio
    new_tickers = set(ticker for ticker, _, _ in top_tickers)
    if existing_tickers != new_tickers:
        # Rebalance the portfolio by selling everything and reallocating
        portfolio_data = []
        investment_per_stock = total_portfolio_value / 10  # Divide the total portfolio value by 10

        for ranking, (ticker, expected_return, last_price) in enumerate(top_tickers):
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

    target_statistics = calculate_price_target_statistics(cursor)
    closing_prices = get_last_closing_price(cursor)

    calculate_and_insert_analysis(cursor, target_statistics, closing_prices)

    # Update the portfolio table with the top 10 tickers
    update_portfolio_table(cursor)

    conn.commit()
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
