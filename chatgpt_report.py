import os
import sys
import mysql.connector
from datetime import datetime
import json
import shutil

# Retrieve MySQL credentials
mdp = os.getenv("MYSQL_MDP")
host = os.getenv("MYSQL_HOST")

if not mdp:
    raise ValueError("No MySQL password found in environment variables")
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

# Establish MySQL connection
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Create a new table for storing the investment reports
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS InvestmentReports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATETIME,
        ticker VARCHAR(10),
        dimension VARCHAR(50),
        question TEXT,
        answer TEXT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

# Function to check for .txt files in ./files directory and subfolders
def find_files_in_directory():
    files_to_process = []
    base_dir = './files'  # Relative path to the 'files' folder
    for subdir, dirs, files in os.walk(base_dir):
        print(f"Checking directory: {subdir}")
        for file in files:
            if file.endswith('.txt'):  # Process only .txt files
                print(f"Found file: {file} in {subdir}")
                file_path = os.path.join(subdir, file)
                ticker = os.path.basename(subdir)  # Subfolder name is the ticker
                files_to_process.append((file_path, ticker))
    return files_to_process

# Function to insert the report into the database
def insert_report_into_db(ticker, json_data):
    for dimension in ['Strengths', 'Weaknesses', 'Opportunities', 'Risks']:
        for entry in json_data[dimension]:
            question = entry['question']
            answer = entry['answer']
            date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            query = """
            INSERT INTO InvestmentReports (date, ticker, dimension, question, answer)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (date_now, ticker, dimension, question, answer))
    
    conn.commit()
    print(f"Inserted data for {ticker} into InvestmentReports")

# Function to move processed files to the ./processed directory
def move_processed_file(file_path):
    processed_dir = './processed'  # Change to relative path for the 'processed' folder
    os.makedirs(processed_dir, exist_ok=True)  # Create the directory if it doesn't exist
    shutil.move(file_path, os.path.join(processed_dir, os.path.basename(file_path)))
    print(f"Moved {file_path} to {processed_dir}")

# Main process function
def process_files():
    create_table()  # Ensure the table exists
    files_to_process = find_files_in_directory()

    if not files_to_process:
        print("No files found in ./files directory.")
        return

    for file_path, ticker in files_to_process:
        print(f"Processing file: {file_path} for ticker: {ticker}")
        try:
            # Read the JSON data from the file
            with open(file_path, 'r') as f:
                json_data = json.load(f)

            # Insert the JSON data into the database
            insert_report_into_db(ticker, json_data)

            # Move file to processed directory
            move_processed_file(file_path)
        except Exception as e:
            print(f"Error processing file {file_path} for ticker {ticker}: {e}")

if __name__ == "__main__":
    try:
        process_files()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
