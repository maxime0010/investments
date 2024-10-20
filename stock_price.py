import os
import sys
import requests
import mysql.connector
from datetime import datetime  # Import the datetime module

# Retrieve API key from environment variables
marketdata_api_key = os.getenv("MARKETDATA_API")
if not marketdata_api_key:
    raise ValueError("No MarketData.app API key found in environment variables")

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
    'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE', 
    'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN', 
    'APH', 'ADI', 'ANSS', 'AON', 'APA', 'AAPL', 'AMAT', 'APTV', 'ACGL', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 
    'AZO', 'AVB', 'AVY', 'AXON', 'BKR', 'BALL', 'BAC', 'BK', 'BBWI', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'TECH', 'BIIB', 'BLK', 
    'BX', 'BA', 'BKNG', 'BWA', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF.B', 'BLDR', 'BG', 'BXP', 'CDNS', 'CZR', 'CPT', 'CPB', 'COF', 
    'CAH', 'KMX', 'CCL', 'CARR', 'CTLT', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'COR', 'CNC', 'CNP', 'CF', 'CHRW', 'CRL', 'SCHW', 
    'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 
    'CAG', 'COP', 'ED', 'STZ', 'CEG', 'COO', 'CPRT', 'GLW', 'CPAY', 'CTVA', 'CSGP', 'COST', 'CTRA', 'CRWD', 'CCI', 'CSX', 'CMI', 
    'CVS', 'DHR', 'DRI', 'DVA', 'DAY', 'DECK', 'DE', 'DAL', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DG', 'DLTR', 'D', 'DPZ', 'DOV', 
    'DOW', 'DHI', 'DTE', 'DUK', 'DD', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'ELV', 'EMR', 'ENPH', 'ETR', 'EOG', 'EPAM', 
    'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'ETSY', 'EG', 'EVRG', 'ES', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FDS', 
    'FICO', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FI', 'FMC', 'F', 'FTNT', 'FTV', 'FOXA', 'FOX', 'BEN', 'FCX', 'GRMN', 
    'IT', 'GE', 'GEHC', 'GEV', 'GEN', 'GNRC', 'GD', 'GIS', 'GM', 'GPC', 'GILD', 'GPN', 'GL', 'GDDY', 'GS', 'HAL', 'HIG', 'HAS', 
    'HCA', 'DOC', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUBB', 'HUM', 'HBAN', 
    'HII', 'IBM', 'IEX', 'IDXX', 'ITW', 'INCY', 'IR', 'PODD', 'INTC', 'ICE', 'IFF', 'IP', 'IPG', 'INTU', 'ISRG', 'IVZ', 'INVH', 
    'IQV', 'IRM', 'JBHT', 'JBL', 'JKHY', 'J', 'JNJ', 'JCI', 'JPM', 'JNPR', 'K', 'KVUE', 'KDP', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 
    'KKR', 'KLAC', 'KHC', 'KR', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LDOS', 'LEN', 'LLY', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW', 
    'LULU', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MTCH', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK', 
    'META', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MRNA', 'MHK', 'MOH', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 
    'MOS', 'MSI', 'MSCI', 'NDAQ', 'NTAP', 'NFLX', 'NEM', 'NWSA', 'NWS', 'NEE', 'NKE', 'NI', 'NDSN', 'NSC', 'NTRS', 'NOC', 'NCLH', 
    'NRG', 'NUE', 'NVDA', 'NVR', 'NXPI', 'ORLY', 'OXY', 'ODFL', 'OMC', 'ON', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PANW', 'PARA', 
    'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 
    'PLD', 'PRU', 'PEG', 'PTC', 'PSA', 'PHM', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 
    'RMD', 'RVTY', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 
    'SJM', 'SW', 'SNA', 'SOLV', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STLD', 'STE', 'SYK', 'SMCI', 'SYF', 'SNPS', 'SYY', 'TMUS', 
    'TROW', 'TTWO', 'TPR', 'TRGP', 'TGT', 'TEL', 'TDY', 'TFX', 'TER', 'TSLA', 'TXN', 'TXT', 'TMO', 'TJX', 'TSCO', 'TT', 'TDG', 
    'TRV', 'TRMB', 'TFC', 'TYL', 'TSN', 'USB', 'UBER', 'UDR', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'UHS', 'VLO', 'VTR', 
    'VLTO', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VTRS', 'VICI', 'V', 'VST', 'VMC', 'WRB', 'GWW', 'WAB', 'WBA', 'WMT', 'DIS', 'WBD', 
    'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WY', 'WMB', 'WTW', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZTS'

]

def insert_price_data(price_data):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        add_price = ("INSERT INTO prices (ticker, date, close) "
                     "VALUES (%(ticker)s, %(date)s, %(close)s) "
                     "ON DUPLICATE KEY UPDATE close = VALUES(close)")

        for price in price_data:
            ticker = price['ticker']
            date = price['date']
            close_price = price['close']

            print(f"Processing {ticker} for date {date} with close price {close_price}")

            cursor.execute("SELECT close FROM prices WHERE ticker = %s AND date = %s", (ticker, date))
            existing_close_price = cursor.fetchone()

            if existing_close_price is None or existing_close_price[0] == 0:
                cursor.execute(add_price, price)
                print(f"Inserted/Updated {ticker} on {date} with close price {close_price}")
            else:
                print(f"Skipped {ticker} on {date} as the close price is already non-zero in the database")

        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def is_data_up_to_date():
    """Check if today's data already exists in the database and ensure no zero values exist."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        today_date = datetime.utcnow().strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT ticker, close
            FROM prices
            WHERE date = %s
        """, (today_date,))

        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        if not rows or any(row['close'] == 0 for row in rows):
            return False

        return True
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


def fetch_sp500_index():
    """Fetch the latest S&P 500 index value."""
    index_symbol = "SPX"  # S&P 500 symbol for the API
    base_url = f"https://api.marketdata.app/v1/indices/quotes/{index_symbol}/"
    
    try:
        headers = {"Authorization": f"Bearer {marketdata_api_key}"}
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        if data.get("s") == "ok":
            last_price = data['last'][0]
            timestamp = int(data['updated'][0])
            date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

            sp500_data = {
                'ticker': 'SPX',
                'date': date,
                'close': last_price
            }

            print(f"Fetched S&P 500 value: {last_price} on {date}")
            return sp500_data
        else:
            print(f"Failed to fetch S&P 500 index data")
            return None

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_and_store_prices(tickers):
    if is_data_up_to_date():
        print("Today's data is already up-to-date. No API call made.")
        return

    base_url = "https://api.marketdata.app/v1/stocks/bulkquotes/"
    symbols = ','.join(tickers)
    price_data = []

    try:
        headers = {"Authorization": f"Bearer {marketdata_api_key}"}
        params = {"symbols": symbols}
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("s") == "ok" and "symbol" in data:
            for idx, ticker in enumerate(data.get("symbol", [])):
                close_price = data['last'][idx]
                if close_price is not None and close_price > 0:
                    timestamp = int(data['updated'][idx])
                    date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

                    price_data.append({
                        'ticker': ticker,
                        'date': date,
                        'close': close_price
                    })
                    print(f"Prepared data for {ticker} on {date} with close price {close_price}")
            print(f"Fetched and prepared data for {len(price_data)} tickers.")
        else:
            print(f"No valid data returned for tickers: {tickers}")
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for tickers {tickers}: {http_err}")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Fetch S&P 500 index and add it to the price data
    sp500_data = fetch_sp500_index()
    if sp500_data:
        price_data.append(sp500_data)

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
