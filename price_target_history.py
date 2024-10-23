import os
import sys
import mysql.connector
from datetime import datetime, timedelta
from benzinga import financial_data

# Retrieve API key and MySQL credentials from environment variables
token = os.getenv("BENZINGA_API_KEY")
mdp = os.getenv("MYSQL_MDP")
host = os.getenv("MYSQL_HOST")

if not token or not mdp or not host:
    raise ValueError("API key or MySQL credentials missing in environment variables")

# Initialize Benzinga API client
bz = financial_data.Benzinga(token)

# Database connection configuration
db_config = {
    'user': 'doadmin',
    'password': mdp,
    'host': host,
    'database': 'defaultdb',
    'port': 25060
}

# List of tickers to fetch ratings for
tickers = [


    'MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE' 
#    'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN' 
#    'APH', 'ADI', 'ANSS', 'AON', 'APA', 'AAPL', 'AMAT', 'APTV', 'ACGL', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP' 
#    'AZO', 'AVB', 'AVY', 'AXON', 'BKR', 'BALL', 'BAC', 'BK', 'BBWI', 'BAX', 'BDX', 'BRK.B', 'BBY', 'BIO', 'TECH', 'BIIB', 'BLK' 
#    'BX', 'BA', 'BKNG', 'BWA', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF.B', 'BLDR', 'BG', 'BXP', 'CDNS', 'CZR', 'CPT', 'CPB', 'COF' 
#    'CAH', 'KMX', 'CCL', 'CARR', 'CTLT', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'COR', 'CNC', 'CNP', 'CF', 'CHRW', 'CRL', 'SCHW'
#    'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA' 
#    'CAG', 'COP', 'ED', 'STZ', 'CEG', 'COO', 'CPRT', 'GLW', 'CPAY', 'CTVA', 'CSGP', 'COST', 'CTRA', 'CRWD', 'CCI', 'CSX', 'CMI'
#    'CVS', 'DHR', 'DRI', 'DVA', 'DAY', 'DECK', 'DE', 'DAL', 'DVN', 'DXCM', 'FANG', 'DLR', 'DFS', 'DG', 'DLTR', 'D', 'DPZ', 'DOV'
#    'DOW', 'DHI', 'DTE', 'DUK', 'DD', 'EMN', 'ETN', 'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'ELV', 'EMR', 'ENPH', 'ETR', 'EOG', 'EPAM'
#    'EQT', 'EFX', 'EQIX', 'EQR', 'ESS', 'EL', 'ETSY', 'EG', 'EVRG', 'ES', 'EXC', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FDS'
#    'FICO', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FI', 'FMC', 'F', 'FTNT', 'FTV', 'FOXA', 'FOX', 'BEN', 'FCX', 'GRMN'
#    'IT', 'GE', 'GEHC', 'GEV', 'GEN', 'GNRC', 'GD', 'GIS', 'GM', 'GPC', 'GILD', 'GPN', 'GL', 'GDDY', 'GS', 'HAL', 'HIG', 'HAS'
#    'HCA', 'DOC', 'HSIC', 'HSY', 'HES', 'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUBB', 'HUM', 'HBAN'
#    'HII', 'IBM', 'IEX', 'IDXX', 'ITW', 'INCY', 'IR', 'PODD', 'INTC', 'ICE', 'IFF', 'IP', 'IPG', 'INTU', 'ISRG', 'IVZ', 'INVH'
#    'IQV', 'IRM', 'JBHT', 'JBL', 'JKHY', 'J', 'JNJ', 'JCI', 'JPM', 'JNPR', 'K', 'KVUE', 'KDP', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI'
#    'KKR', 'KLAC', 'KHC', 'KR', 'LHX', 'LH', 'LRCX', 'LW', 'LVS', 'LDOS', 'LEN', 'LLY', 'LIN', 'LYV', 'LKQ', 'LMT', 'L', 'LOW'
#    'LULU', 'LYB', 'MTB', 'MRO', 'MPC', 'MKTX', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MTCH', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK'
#    'META', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MRNA', 'MHK', 'MOH', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS'
#    'MOS', 'MSI', 'MSCI', 'NDAQ', 'NTAP', 'NFLX', 'NEM', 'NWSA', 'NWS', 'NEE', 'NKE', 'NI', 'NDSN', 'NSC', 'NTRS', 'NOC', 'NCLH'
#    'NRG', 'NUE', 'NVDA', 'NVR', 'NXPI', 'ORLY', 'OXY', 'ODFL', 'OMC', 'ON', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PANW', 'PARA'
#    'PH', 'PAYX', 'PAYC', 'PYPL', 'PNR', 'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR'
#    'PLD', 'PRU', 'PEG', 'PTC', 'PSA', 'PHM', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG'
#    'RMD', 'RVTY', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SBAC', 'SLB', 'STX', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS'
#    'SJM', 'SW', 'SNA', 'SOLV', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STLD', 'STE', 'SYK', 'SMCI', 'SYF', 'SNPS', 'SYY', 'TMUS'
#    'TROW', 'TTWO', 'TPR', 'TRGP', 'TGT', 'TEL', 'TDY', 'TFX', 'TER', 'TSLA', 'TXN', 'TXT', 'TMO', 'TJX', 'TSCO', 'TT', 'TDG'
#    'TRV', 'TRMB', 'TFC', 'TYL', 'TSN', 'USB', 'UBER', 'UDR', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'UHS', 'VLO', 'VTR'
#    'VLTO', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VTRS', 'VICI', 'V', 'VST', 'VMC', 'WRB', 'GWW', 'WAB', 'WBA', 'WMT', 'DIS', 'WBD'
#    'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WY', 'WMB', 'WTW', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZTS'


]

def safe_cast(value, target_type, default=None):
    """Safely cast values to the target type, with a fallback to a default value if casting fails."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default

def insert_rating_data(rating_data, cursor):
    """Insert rating data into the MySQL database with added debug statements."""
    try:
        add_rating = ("""
            INSERT INTO ratings 
            (id, action_company, action_pt, adjusted_pt_current, adjusted_pt_prior, analyst, analyst_name,
             currency, date, exchange, importance, name, notes, pt_current, pt_prior, rating_current, 
             rating_prior, ticker, time, updated, url, url_calendar, url_news) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            adjusted_pt_current = VALUES(adjusted_pt_current), 
            adjusted_pt_prior = VALUES(adjusted_pt_prior), 
            pt_current = VALUES(pt_current), 
            pt_prior = VALUES(pt_prior), 
            rating_current = VALUES(rating_current),
            rating_prior = VALUES(rating_prior), 
            updated = VALUES(updated)
        """)

        added_ratings = 0
        for rating in rating_data:
            # Safely cast each value to the appropriate data type and handle missing keys
            rating['adjusted_pt_current'] = safe_cast(rating.get('adjusted_pt_current'), float, None)
            rating['adjusted_pt_prior'] = safe_cast(rating.get('adjusted_pt_prior'), float, None)
            rating['pt_current'] = safe_cast(rating.get('pt_current'), float, None)
            rating['pt_prior'] = safe_cast(rating.get('pt_prior'), float, None)
            
            # Debug: Print the rating data before inserting
            print(f"Attempting to insert rating: {rating}")
            
            # Insert rating data, providing default values for missing fields
            try:
                cursor.execute(add_rating, (
                    rating.get('id', None), rating.get('action_company', ''), rating.get('action_pt', ''),
                    rating['adjusted_pt_current'], rating['adjusted_pt_prior'], rating.get('analyst', ''),
                    rating.get('analyst_name', ''), rating.get('currency', ''), rating.get('date', ''),
                    rating.get('exchange', ''), rating.get('importance', 0), rating.get('name', ''),
                    rating.get('notes', ''), rating['pt_current'], rating['pt_prior'],
                    rating.get('rating_current', ''), rating.get('rating_prior', ''), rating.get('ticker', ''),
                    rating.get('time', ''), rating.get('updated', ''), rating.get('url', ''),
                    rating.get('url_calendar', ''), rating.get('url_news', '')
                ))
                added_ratings += 1
            except mysql.connector.Error as err:
                # Debug: Print error details
                print(f"Error inserting rating data for {rating.get('ticker', '')} at {rating.get('date', '')}: {err}")
                # Correctly format the SQL statement debug output
                sql_statement = cursor.mogrify(add_rating, (
                    rating.get('id', None), rating.get('action_company', ''), rating.get('action_pt', ''),
                    rating['adjusted_pt_current'], rating['adjusted_pt_prior'], rating.get('analyst', ''),
                    rating.get('analyst_name', ''), rating.get('currency', ''), rating.get('date', ''),
                    rating.get('exchange', ''), rating.get('importance', 0), rating.get('name', ''),
                    rating.get('notes', ''), rating['pt_current'], rating['pt_prior'],
                    rating.get('rating_current', ''), rating.get('rating_prior', ''), rating.get('ticker', ''),
                    rating.get('time', ''), rating.get('updated', ''), rating.get('url', ''),
                    rating.get('url_calendar', ''), rating.get('url_news', '')
                ))
                print(f"SQL Statement: {sql_statement.decode('utf-8')}")
                continue  # Skip to the next rating if there's an issue with the current one

        return added_ratings

    except mysql.connector.Error as err:
        print(f"Error inserting rating data: {err}")
        return 0



def fetch_ratings_for_september(ticker, cursor):
    """Fetch ratings for September 2024 for the given ticker."""
    # Set the start and end date for September 2024
    date_from = "2013-01-01"
    date_to = "2013-12-31"

    params = {
        'company_tickers': ticker,
        'date_from': date_from,
        'date_to': date_to
    }

    print(f"Fetching ratings for {ticker} from {date_from} to {date_to}...")

    try:
        rating_data = bz.ratings(**params)
        if rating_data and 'ratings' in rating_data and rating_data['ratings']:
            added_ratings = insert_rating_data(rating_data['ratings'], cursor)
            print(f"Successfully added {added_ratings} ratings for {ticker}")
        else:
            print(f"No ratings found for {ticker} in September 2024")
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")

def fetch_and_store_ratings(tickers):
    """Fetch and store ratings for each ticker for September 2024."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Loop through each ticker and fetch ratings for September 2024
        for ticker in tickers:
            fetch_ratings_for_september(ticker, cursor)

        conn.commit()
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def exit_program():
    """Exit the program."""
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_ratings(tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
