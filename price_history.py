import os
import mysql.connector
import pandas as pd
from glob import glob

# Retrieve MySQL password and host from environment variables
mdp = os.getenv("MYSQL_MDP")
host = os.getenv("MYSQL_HOST")

if not mdp or not host:
    raise ValueError("MySQL credentials missing in environment variables")

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
    
    'SPX'


]

# Define the folder path containing the CSV files
csv_folder = "csv"

# Check if the CSV folder exists and contains files
if not os.path.exists(csv_folder):
    print(f"CSV folder {csv_folder} does not exist.")
else:
    print(f"CSV folder {csv_folder} found.")
    csv_files = glob(os.path.join(csv_folder, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_folder}.")
    else:
        print(f"Found {len(csv_files)} CSV files in {csv_folder}.")

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Prepare MySQL table
cursor.execute("""
CREATE TABLE IF NOT EXISTS prices (
    ticker VARCHAR(10),
    date DATE,
    close DECIMAL(10, 2),
    PRIMARY KEY (ticker, date)
)
""")
conn.commit()

# Define batch size for bulk insertion
BATCH_SIZE = 1000

# Function to insert data in batches
def insert_data_in_batches(data, cursor):
    placeholders = ', '.join(['(%s, %s, %s)'] * len(data))
    query = f"INSERT INTO prices (ticker, date, close) VALUES {placeholders} ON DUPLICATE KEY UPDATE close = VALUES(close)"
    flattened_data = [item for sublist in data for item in sublist]
    cursor.execute(query, flattened_data)

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

        # Ensure the date column is in datetime format and remove dollar sign from 'Close/Last'
        try:
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
            df['Close/Last'] = df['Close/Last'].replace({'\$': ''}, regex=True).astype(float)
        except Exception as e:
            print(f"Error processing data in {csv_file}: {e}")
            continue

        # Add a column for the ticker
        df['Ticker'] = ticker

        # Prepare data for batch insertion
        data_batch = []
        for _, row in df.iterrows():
            data_batch.append((ticker, row['Date'].date(), row['Close/Last']))

            # Insert data in batches
            if len(data_batch) >= BATCH_SIZE:
                try:
                    insert_data_in_batches(data_batch, cursor)
                    conn.commit()
                    print(f"Inserted {len(data_batch)} records for {ticker}")
                    data_batch.clear()
                except Exception as e:
                    print(f"Error inserting data for {ticker}: {e}")
                    data_batch.clear()

        # Insert remaining data if any
        if data_batch:
            try:
                insert_data_in_batches(data_batch, cursor)
                conn.commit()
                print(f"Inserted remaining {len(data_batch)} records for {ticker}")
            except Exception as e:
                print(f"Error inserting remaining data for {ticker}: {e}")

    else:
        print(f"Ticker {ticker} is NOT in the S&P 500 list, skipping.")

# Close the connection
cursor.close()
conn.close()
print("Database connection closed.")
