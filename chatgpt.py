import os
import sys
import mysql.connector
from openai import OpenAI
from datetime import datetime, timedelta

# Retrieve API keys and MySQL credentials
chatgpt_key = os.getenv("CHATGPT_KEY")
mdp = os.getenv("MYSQL_MDP")
host = os.getenv("MYSQL_HOST")

if not chatgpt_key:
    raise ValueError("No ChatGPT key found in environment variables")
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

# Initialize OpenAI client
openai_client = OpenAI(api_key=chatgpt_key)

# Establish MySQL connection
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Function to fetch expected return from 'analysis_simulation' table
def fetch_expected_return(ticker):
    query = """
        SELECT expected_return_combined_criteria
        FROM analysis_simulation
        WHERE ticker = %s
        ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to check if a recommendation has been made in the last week
def is_recent_entry(ticker):
    one_week_ago = datetime.now() - timedelta(weeks=1)
    query = """
        SELECT date
        FROM chatgpt
        WHERE ticker = %s
        ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    if result:
        last_entry_date = result[0]
        return last_entry_date >= one_week_ago
    return False

# Function to generate recommendations using ChatGPT as the top analyst
def generate_recommendations(ticker, expected_return):
    # Generate short recommendation
    short_prompt = f"You are a top analyst. Based on public information and the expected return of {expected_return}% for {ticker}, give a short 20-word recommendation."
    short_response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": short_prompt}]
    )
    short_recommendation = short_response.choices[0].message.content

    # Generate long recommendation
    long_prompt = f"As a top analyst, give a detailed 200-word recommendation for {ticker}. The expected return is {expected_return}%. Use public information and market trends to justify your analysis."
    long_response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": long_prompt}]
    )
    long_recommendation = long_response.choices[0].message.content

    return short_recommendation, long_recommendation

# Function to insert recommendations into 'chatgpt' table
def insert_recommendation(ticker, short_recommendation, long_recommendation):
    query = """
        INSERT INTO chatgpt (date, ticker, short_recommendation, long_recommendation)
        VALUES (%s, %s, %s, %s)
    """
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(query, (date, ticker, short_recommendation, long_recommendation))
    conn.commit()

# Main script to process stock data
def process_stocks(tickers):
    for ticker in tickers:
        try:
            # Step 1: Check if a recent recommendation exists
            if is_recent_entry(ticker):
                print(f"Recent recommendation exists for {ticker}, skipping.")
                continue

            # Step 2: Fetch expected return from the database
            expected_return = fetch_expected_return(ticker)
            if expected_return is None:
                print(f"No expected return data for {ticker}. Skipping.")
                continue

            # Step 3: Generate recommendations using ChatGPT
            short_recommendation, long_recommendation = generate_recommendations(ticker, expected_return)

            # Step 4: Store the result in 'chatgpt' table
            insert_recommendation(ticker, short_recommendation, long_recommendation)

            print(f"Processed {ticker}")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

# Create the 'chatgpt' table if it does not exist
def create_chatgpt_table():
    query = """
    CREATE TABLE IF NOT EXISTS chatgpt (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATETIME,
        ticker VARCHAR(10),
        short_recommendation TEXT,
        long_recommendation TEXT
    )
    """
    cursor.execute(query)
    conn.commit()

# Script execution
if __name__ == "__main__":
    try:
        create_chatgpt_table()
        sp500_tickers = [    
            'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE'            'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WY', 'WMB', 'WTW', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZTS'
        ]
        process_stocks(sp500_tickers)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
