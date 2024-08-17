import os
import mysql.connector
from datetime import datetime

# Retrieve MySQL password from environment variables
mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")

# Database connection
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': 'db-mysql-nyc3-03005-do-user-4526552-0.h.db.ondigitalocean.com',
    'database': 'defaultdb',
    'port': 25060
}

def calculate_price_target_statistics(cursor):
    query = """
        SELECT 
            r.ticker,
            AVG(r.adjusted_pt_current) AS average_price_target,
            STDDEV(r.adjusted_pt_current) AS stddev_price_target,
            COUNT(DISTINCT r.analyst_name) AS num_analysts,
            MAX(r.date) AS last_update_date,
            AVG(DATEDIFF(NOW(), r.date)) AS avg_days_since_last_update,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.analyst_name END) AS num_analysts_last_7_days,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.adjusted_pt_current END) AS stddev_price_target_last_7_days,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.adjusted_pt_current END) AS average_price_target_last_7_days,
            COUNT(DISTINCT CASE WHEN a.overall_success_rate > 50 THEN r.analyst_name END) AS num_high_success_analysts,
            STDDEV(CASE WHEN a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS stddev_high_success_analysts,
            AVG(CASE WHEN a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS avg_high_success_analysts,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.analyst_name END) AS num_combined_criteria,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS stddev_combined_criteria,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS avg_combined_criteria
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
        num_analysts_last_7_days = stats[6]
        stddev_price_target_last_7_days = stats[7]
        average_price_target_last_7_days = stats[8]
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
            
            expected_return_last_7_days = None
            if average_price_target_last_7_days is not None:
                expected_return_last_7_days = ((average_price_target_last_7_days - last_closing_price) / last_closing_price) * 100

            expected_return_high_success = None
            if avg_high_success_analysts is not None:
                expected_return_high_success = ((avg_high_success_analysts - last_closing_price) / last_closing_price) * 100
            
            expected_return_combined_criteria = None
            if avg_combined_criteria is not None:
                expected_return_combined_criteria = ((avg_combined_criteria - last_closing_price) / last_closing_price) * 100

            analysis_data.append((ticker, last_closing_price, average_price_target, expected_return, 
                                  num_analysts, stddev_price_target, days_since_last_update, avg_days_since_last_update,
                                  num_analysts_last_7_days, stddev_price_target_last_7_days, expected_return_last_7_days,
                                  num_high_success_analysts, stddev_high_success_analysts, avg_high_success_analysts, expected_return_high_success,
                                  num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria))

    insert_query = """
        INSERT INTO analysis (ticker, last_closing_price, average_price_target, expected_return, num_analysts, stddev_price_target, days_since_last_update, avg_days_since_last_update, num_analysts_last_7_days, stddev_price_target_last_7_days, expected_return_last_7_days, num_high_success_analysts, stddev_high_success_analysts, avg_high_success_analysts, expected_return_high_success, num_combined_criteria, stddev_combined_criteria, avg_combined_criteria, expected_return_combined_criteria)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            last_closing_price = VALUES(last_closing_price), 
            average_price_target = VALUES(average_price_target), 
            expected_return = VALUES(expected_return),
            num_analysts = VALUES(num_analysts),
            stddev_price_target = VALUES(stddev_price_target),
            days_since_last_update = VALUES(days_since_last_update),
            avg_days_since_last_update = VALUES(avg_days_since_last_update),
            num_analysts_last_7_days = VALUES(num_analysts_last_7_days),
            stddev_price_target_last_7_days = VALUES(stddev_price_target_last_7_days),
            expected_return_last_7_days = VALUES(expected_return_last_7_days),
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

# Script execution
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

target_statistics = calculate_price_target_statistics(cursor)
closing_prices = get_last_closing_price(cursor)

calculate_and_insert_analysis(cursor, target_statistics, closing_prices)

conn.commit()
cursor.close()
conn.close()
