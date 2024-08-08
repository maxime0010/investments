import os
import mysql.connector

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

def calculate_average_price_target(cursor):
    query = """
        SELECT 
            r.ticker,
            AVG(r.adjusted_pt_current) AS average_price_target
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

def calculate_expected_return_and_insert(cursor, average_targets, closing_prices):
    analysis_data = []
    closing_price_dict = {price[0]: price[1] for price in closing_prices}
    
    for target in average_targets:
        ticker = target[0]
        average_price_target = target[1]
        last_closing_price = closing_price_dict.get(ticker)
        if last_closing_price:
            expected_return = ((average_price_target - last_closing_price) / last_closing_price) * 100
            analysis_data.append((ticker, last_closing_price, average_price_target, expected_return))

    insert_query = """
        INSERT INTO analysis (ticker, last_closing_price, average_price_target, expected_return)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            last_closing_price = VALUES(last_closing_price), 
            average_price_target = VALUES(average_price_target), 
            expected_return = VALUES(expected_return)
    """
    cursor.executemany(insert_query, analysis_data)

def main():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        average_targets = calculate_average_price_target(cursor)
        closing_prices = get_last_closing_price(cursor)

        calculate_expected_return_and_insert(cursor, average_targets, closing_prices)

        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
