import os
import requests
import mysql.connector

# Benzinga API token
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API token found in environment variables")

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

def fetch_analysts_data(analyst_names):
    analysts_data = []
    for start in range(0, len(analyst_names), 50):
        batch = analyst_names[start:start+50]
        url = "https://api.benzinga.com/api/v2.1/calendar/ratings/analysts"
        querystring = {"token": token, "analyst_name": ",".join(batch)}
        response = requests.get(url, params=querystring)
        if response.status_code == 200:
            analysts_data.extend(response.json().get('analyst_ratings_analyst', []))
        else:
            print(f"Error fetching data: {response.status_code}")
    return analysts_data

def clean_data(value, default=''):
    return value.strip() if value else default

def insert_analysts_data(cursor, analysts_data):
    add_analyst = ("INSERT INTO analysts (firm_id, firm_name, id, name_first, name_full, name_last, "
                   "one_month_average_return, one_year_success_rate, two_year_success_rate, overall_success_rate, smart_score, total_ratings_percentile) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                   "ON DUPLICATE KEY UPDATE "
                   "firm_name = VALUES(firm_name), "
                   "name_first = VALUES(name_first), "
                   "name_full = VALUES(name_full), "
                   "name_last = VALUES(name_last), "
                   "one_month_average_return = VALUES(one_month_average_return), "
                   "one_year_success_rate = VALUES(one_year_success_rate), "
                   "two_year_success_rate = VALUES(two_year_success_rate), "
                   "overall_success_rate = VALUES(overall_success_rate), "
                   "smart_score = VALUES(smart_score), "
                   "total_ratings_percentile = VALUES(total_ratings_percentile)")

    for analyst in analysts_data:
        ratings_accuracy = analyst.get('ratings_accuracy', {})
        data_tuple = (
            clean_data(analyst.get('firm_id')),
            clean_data(analyst.get('firm_name')),
            clean_data(analyst.get('id')),
            clean_data(analyst.get('name_first')),
            clean_data(analyst.get('name_full')),
            clean_data(analyst.get('name_last')),
            float(ratings_accuracy.get('1m_average_return', 0)),
            float(ratings_accuracy.get('1y_success_rate', 0)),
            float(ratings_accuracy.get('2y_success_rate', 0)),
            float(ratings_accuracy.get('overall_success_rate', 0)),
            float(ratings_accuracy.get('smart_score', 0)),
            float(ratings_accuracy.get('total_ratings_percentile', 0))
        )
        
        try:
            print(f"Inserting data tuple: {data_tuple}")  # Print data tuple before inserting
            cursor.execute(add_analyst, data_tuple)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            print(f"SQL Query: {cursor.statement}")
            print(f"Data tuple: {data_tuple}")

def get_unique_analyst_names(cursor):
    cursor.execute("SELECT DISTINCT analyst_name FROM ratings")
    analyst_names = [row[0] for row in cursor.fetchall()]
    print(f"Unique analysts found: {len(analyst_names)}")
    print("Analyst names:", analyst_names)
    return analyst_names

# Script execution
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

analyst_names = get_unique_analyst_names(cursor)
if analyst_names:
    analysts_data = fetch_analysts_data(analyst_names)
    insert_analysts_data(cursor, analysts_data)

conn.commit()
cursor.close()
conn.close()
