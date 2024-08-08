import os
import requests
import mysql.connector

# Retrieve MySQL password from environment variables
mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")

# Benzinga API token
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API token found in environment variables")

# Database connection
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': 'db-mysql-nyc3-03005-do-user-4526552-0.h.db.ondigitalocean.com',
    'database': 'defaultdb',
    'port': 25060
}

def fetch_analysts_data():
    url = "https://api.benzinga.com/api/v2.1/calendar/ratings/analysts"
    querystring = {"token": token}
    response = requests.get(url, params=querystring)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

def insert_analysts_data(cursor, analysts_data):
    add_analyst = ("INSERT INTO analysts (firm_id, firm_name, id, name_first, name_full, name_last, "
                   "one_month_average_return, one_month_smart_score, one_month_stdev, one_month_success_rate, "
                   "one_year_average_return, one_year_smart_score, one_year_stdev, one_year_success_rate, "
                   "two_year_average_return, two_year_smart_score, two_year_stdev, two_year_success_rate, "
                   "three_month_average_return, three_month_smart_score, three_month_stdev, three_month_success_rate, "
                   "three_year_average_return, three_year_smart_score, three_year_stdev, three_year_success_rate, "
                   "nine_month_average_return, nine_month_smart_score, nine_month_stdev, nine_month_success_rate, "
                   "overall_average_return, overall_avg_return_percentile, overall_stdev, overall_success_rate, "
                   "smart_score, total_ratings_percentile, updated) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                   "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                   "ON DUPLICATE KEY UPDATE "
                   "firm_id = VALUES(firm_id), firm_name = VALUES(firm_name), name_first = VALUES(name_first), "
                   "name_full = VALUES(name_full), name_last = VALUES(name_last), "
                   "one_month_average_return = VALUES(one_month_average_return), one_month_smart_score = VALUES(one_month_smart_score), "
                   "one_month_stdev = VALUES(one_month_stdev), one_month_success_rate = VALUES(one_month_success_rate), "
                   "one_year_average_return = VALUES(one_year_average_return), one_year_smart_score = VALUES(one_year_smart_score), "
                   "one_year_stdev = VALUES(one_year_stdev), one_year_success_rate = VALUES(one_year_success_rate), "
                   "two_year_average_return = VALUES(two_year_average_return), two_year_smart_score = VALUES(two_year_smart_score), "
                   "two_year_stdev = VALUES(two_year_stdev), two_year_success_rate = VALUES(two_year_success_rate), "
                   "three_month_average_return = VALUES(three_month_average_return), three_month_smart_score = VALUES(three_month_smart_score), "
                   "three_month_stdev = VALUES(three_month_stdev), three_month_success_rate = VALUES(three_month_success_rate), "
                   "three_year_average_return = VALUES(three_year_average_return), three_year_smart_score = VALUES(three_year_smart_score), "
                   "three_year_stdev = VALUES(three_year_stdev), three_year_success_rate = VALUES(three_year_success_rate), "
                   "nine_month_average_return = VALUES(nine_month_average_return), nine_month_smart_score = VALUES(nine_month_smart_score), "
                   "nine_month_stdev = VALUES(nine_month_stdev), nine_month_success_rate = VALUES(nine_month_success_rate), "
                   "overall_average_return = VALUES(overall_average_return), overall_avg_return_percentile = VALUES(overall_avg_return_percentile), "
                   "overall_stdev = VALUES(overall_stdev), overall_success_rate = VALUES(overall_success_rate), "
                   "smart_score = VALUES(smart_score), total_ratings_percentile = VALUES(total_ratings_percentile), updated = VALUES(updated)")
    
    for analyst in analysts_data.get('analyst_ratings_analyst', []):
        ratings_accuracy = analyst.get('ratings_accuracy', {})
        cursor.execute(add_analyst, (
            analyst.get('firm_id'),
            analyst.get('firm_name'),
            analyst.get('id'),
            analyst.get('name_first'),
            analyst.get('name_full'),
            analyst.get('name_last'),
            float(ratings_accuracy.get('1m_average_return', 0)),
            float(ratings_accuracy.get('1m_smart_score', 0)),
            float(ratings_accuracy.get('1m_stdev', 0)),
            float(ratings_accuracy.get('1m_success_rate', 0)),
            float(ratings_accuracy.get('1y_average_return', 0)),
            float(ratings_accuracy.get('1y_smart_score', 0)),
            float(ratings_accuracy.get('1y_stdev', 0)),
            float(ratings_accuracy.get('1y_success_rate', 0)),
            float(ratings_accuracy.get('2y_average_return', 0)),
            float(ratings_accuracy.get('2y_smart_score', 0)),
            float(ratings_accuracy.get('2y_stdev', 0)),
            float(ratings_accuracy.get('2y_success_rate', 0)),
            float(ratings_accuracy.get('3m_average_return', 0)),
            float(ratings_accuracy.get('3m_smart_score', 0)),
            float(ratings_accuracy.get('3m_stdev', 0)),
            float(ratings_accuracy.get('3m_success_rate', 0)),
            float(ratings_accuracy.get('3y_average_return', 0)),
            float(ratings_accuracy.get('3y_smart_score', 0)),
            float(ratings_accuracy.get('3y_stdev', 0)),
            float(ratings_accuracy.get('3y_success_rate', 0)),
            float(ratings_accuracy.get('9m_average_return', 0)),
            float(ratings_accuracy.get('9m_smart_score', 0)),
            float(ratings_accuracy.get('9m_stdev', 0)),
            float(ratings_accuracy.get('9m_success_rate', 0)),
            float(ratings_accuracy.get('overall_average_return', 0)),
            float(ratings_accuracy.get('overall_avg_return_percentile', 0)),
            float(ratings_accuracy.get('overall_stdev', 0)),
            float(ratings_accuracy.get('overall_success_rate', 0)),
            float(ratings_accuracy.get('smart_score', 0)),
            float(ratings_accuracy.get('total_ratings_percentile', 0)),
            analyst.get('updated')
        ))

def main():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        analysts_data = fetch_analysts_data()
        if analysts_data:
            insert_analysts_data(cursor, analysts_data)

        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
