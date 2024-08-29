def insert_price_data(price_data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        add_price = ("INSERT INTO prices (ticker, date, close) "
                     "VALUES (%(ticker)s, %(date)s, %(close)s) "
                     "ON DUPLICATE KEY UPDATE close = VALUES(close)")

        for price in price_data:
            cursor.execute(add_price, price)
        
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def is_data_up_to_date():
    """Check if today's data already exists in the database."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        today_date = datetime.utcnow().strftime('%Y-%m-%d')

        # Query the database for the most recent entry
        cursor.execute("SELECT MAX(date) FROM prices")
        last_date = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        # Check if the last_date is today
        if last_date and last_date.strftime('%Y-%m-%d') == today_date:
            return True
        return False

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False

def fetch_and_store_prices(tickers):
    if is_data_up_to_date():
        print("Today's data is already up-to-date. No API call made.")
        return

    base_url = "https://api.marketdata.app/v1/stocks/bulkquotes/"
    symbols = ','.join(tickers)
    price_data = []

    try:
        headers = {
            "Authorization": f"Bearer {marketdata_api_key}"
        }
        params = {
            "symbols": symbols
        }
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if data["s"] == "ok":
            for idx, ticker in enumerate(data["symbol"]):
                # Convert Unix timestamp to date
                timestamp = int(data['updated'][idx])
                date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

                price_data.append({
                    'ticker': ticker,
                    'date': date,  # Use the converted date
                    'close': data['last'][idx]
                })
            print(f"Fetched and prepared data for {len(tickers)} tickers.")
        else:
            print(f"No data returned for tickers: {tickers}")
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for tickers {tickers}: {http_err}")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    if price_data:
        insert_price_data(price_data)

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_prices(sp500_tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
