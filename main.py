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
    add_analyst = ("INSERT INTO analysts (firm_id, firm_name, id) "
                   "VALUES (%s, %s, %s) "
                   "ON DUPLICATE KEY UPDATE "
                   "firm_name = VALUES(firm_name)")

    for analyst in analysts_data.get('analyst_ratings_analyst', []):
        data_tuple = (
            analyst.get('firm_id'),
            analyst.get('firm_name'),
            analyst.get('id')
        )
        
        try:
            print(f"Inserting data tuple: {data_tuple}")  # Print data tuple before inserting
            cursor.execute(add_analyst, data_tuple)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            print(f"SQL Query: {cursor.statement}")
            print(f"Data tuple: {data_tuple}")

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
