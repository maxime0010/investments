import os
import mysql.connector
from datetime import datetime, timedelta
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

def get_latest_simulation_date(cursor):
    """Fetch the latest simulation date from the analysis_simulation table."""
    query = "SELECT MAX(date) FROM analysis_simulation"
    cursor.execute(query)
    latest_date = cursor.fetchone()[0]
    if latest_date is None:
        # If no simulation data exists, start from the initial date
        return datetime(2019, 9, 1).date()  # Adjust the start date as needed
    else:
        return latest_date

def calculate_price_target_statistics(cursor, analysis_date):
    # Adjust query to use the specific date for historical simulation
    query = f"""
        SELECT 
            r.ticker,
            AVG(r.adjusted_pt_current) AS average_price_target,
            STDDEV(r.adjusted_pt_current) AS stddev_price_target,
            COUNT(DISTINCT r.analyst_name) AS num_analysts,
            MAX(r.date) AS last_update_date,
            AVG(DATEDIFF(%(analysis_date)s, r.date)) AS avg_days_since_last_update,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) THEN r.analyst_name END) AS num_recent_analysts,
            STDDEV(CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) THEN r.adjusted_pt_current END) AS stddev_price_target_recent,
            AVG(CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) THEN r.adjusted_pt_current END) AS average_price_target_recent,
            COUNT(DISTINCT CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.analyst_name END) AS num_high_success_analysts,
            STDDEV(CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS stddev_high_success_analysts,
            AVG(CASE WHEN a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS avg_high_success_analysts,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.analyst_name END) AS num_combined_criteria,
            STDDEV(CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS stddev_combined_criteria,
            AVG(CASE WHEN r.date >= DATE_SUB(%(analysis_date)s, INTERVAL {DAYS_RECENT} DAY) AND a.overall_success_rate > {SUCCESS_RATE_THRESHOLD} THEN r.adjusted_pt_current END) AS avg_combined_criteria
        FROM (
            SELECT 
                ticker,
                analyst_name,
                MAX(date) AS latest_date
            FROM ratings
            WHERE date <= %(analysis_date)s
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
    cursor.execute(query, {'analysis_date': analysis_date})
    return cursor.fetchall()

def get_closing_price_as_of(cursor, analysis_date):
    query = """
        SELECT 
            ticker,
            close AS last_closing_price
        FROM prices
        WHERE (ticker, date) IN (
            SELECT ticker, MAX(date) 
            FROM prices 
            WHERE date <= %(analysis_date)s
            GROUP BY ticker
        )
    """
    cursor.execute(query, {'analysis_date': analysis_date})
    return cursor.fetchall()

def calculate_and_insert_simulated_analysis(cursor, target_statistics, closing_prices, analysis_date):
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
            days_since_last_update = (analysis_date - last_update_date).days
            
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
                                  num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria, analysis_date))

    insert_query = """
        INSERT INTO analysis_simulation (ticker, last_closing_price, average_price_target, expected_return, num_analysts, stddev_price_target, days_since_last_update, avg_days_since_last_update, num_recent_analysts, stddev_price_target_recent, expected_return_recent, num_high_success_analysts, stddev_high_success_analysts, avg_high_success_analysts, expected_return_high_success, num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, analysis_data)

def simulate_portfolio_performance():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Get the latest date from the simulation table
    latest_simulation_date = get_latest_simulation_date(cursor)
    print(f"Latest simulation date: {latest_simulation_date}")

    start_date = latest_simulation_date + timedelta(weeks=1)  # Start from one week after the latest simulation date
    end_date = datetime.now()
    current_date = start_date

    try:
        while current_date <= end_date:
            print(f"Running simulation for {current_date.date()}...")
            
            # Calculate statistics and prices as of the current date
            target_statistics = calculate_price_target_statistics(cursor, current_date)
            closing_prices = get_closing_price_as_of(cursor, current_date)

            # Insert the analysis results into the simulation table
            calculate_and_insert_simulated_analysis(cursor, target_statistics, closing_prices, current_date.date())

            # Move to the next week
            current_date += timedelta(weeks=1)

            # Commit after each week’s simulation
            conn.commit()
        
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        cursor.close()
        conn.close()

# Run the simulation
simulate_portfolio_performance()
