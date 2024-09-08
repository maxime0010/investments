import os
import mysql.connector
import pandas as pd
from glob import glob

# Retrieve MySQL password and host from environment variables
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

# List of S&P 500 tickers
sp500_tickers = [
    'MMM', 'AOS', 'ABT'
]

# Define the folder path containing the CSV files
csv_folder = "csv"

# Check if the CSV folder exists and contains files
if not os.path.exists(csv_folder):
    print(f"CSV folder {csv_folder} does not exist.")
else:
    print(f"CSV folder {csv_folder} found.")

    # Check if any CSV files exist in the folder
    csv_files = glob(os.path.join(csv_folder, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_folder}.")
    else:
        print(f"Found {len(csv_files)} CSV files in {csv_folder}.")

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Create the prices table if it doesn't exist
print("Creating table 'prices' if not exists...")
cursor.execute("""
CREATE TABLE IF NOT EXISTS prices (
    ticker VARCHAR(10),
    date DATE,
    close DECIMAL(10, 2),
    PRIMARY KEY (ticker, date)
)
""")
conn.commit()
print("Table check/creation completed.")

# Loop through each CSV file
for csv_file in csv_files:
    # Extract the ticker from the filename
    ticker = os.path.basename(csv_file).split('.')[0].upper()
    print(f"Processing CSV for ticker: {ticker}")

    # Check if the ticker is in the S&P 500 list
    if ticker in sp500_tickers:
        print(f"Ticker {ticker} is in the S&P 500 list.")
        # Read the CSV file into a DataFrame
        try:
            df = pd.read_csv(csv_file)
            print(f"Read {len(df)} rows from {csv_file}.")
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
            continue

        # Ensure the date column is in datetime format
        try:
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
        except Exception as e:
            print(f"Error converting date in {csv_file}: {e}")
            continue

        # Remove dollar signs and convert the close prices to float
        df['Close/Last'] = df['Close/Last'].replace({'\$': ''}, regex=True).astype(float)

        # Loop through each row in the DataFrame
        for _, row in df.iterrows():
            date = row['Date'].date()
            close = row['Close/Last']

            print(f"Inserting/updating data for {ticker} on {date}: Close = {close}")
            
            # Insert or update the data in the prices table
            try:
                cursor.execute("""
                INSERT INTO prices (ticker, date, close)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE close = VALUES(close)
                """, (ticker, date, close))
            except Exception as e:
                print(f"Error inserting data for {ticker} on {date}: {e}")
                continue
    else:
        print(f"Ticker {ticker} is NOT in the S&P 500 list, skipping.")

# Commit the transaction and close the connection
conn.commit()
print("Data insertion/update completed.")
cursor.close()
conn.close()
print("Database connection closed.")
