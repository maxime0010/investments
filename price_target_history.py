import os
import sys
import mysql.connector
from datetime import datetime, timedelta
from benzinga import financial_data

# Retrieve API key from environment variables
token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

bz = financial_data.Benzinga(token)

# List of S&P 500 tickers (as provided)
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
    'password': os.getenv("MYSQL_MDP"),
    'host': os.getenv("MYSQL_HOST"),
    'database': 'defaultdb',
    'port': 25060
}

def insert_rating_data(rating_data, cursor):
    added_ratings = 0
    try:
        add_rating = ("INSERT INTO ratings "
                      "(id, action_company, action_pt, adjusted_pt_current, adjusted_pt_prior, analyst, analyst_name, "
                      "currency, date, exchange, importance, name, notes, pt_current, pt_prior, rating_current, "
                      "rating_prior, ticker, time, updated, url, url_calendar, url_news) "
                      "VALUES (%(id)s, %(action_company)s, %(action_pt)s, %(adjusted_pt_current)s, %(adjusted_pt_prior)s, "
                      "%(analyst)s, %(analyst_name)s, %(currency)s, %(date)s, %(exchange)s, %(importance)s, %(name)s, "
                      "%(notes)s, %(pt_current)s, %(pt_prior)s, %(rating_current)s, %(rating_prior)s, %(ticker)s, "
                      "%(time)s, %(updated)s, %(url)s, %(url_calendar)s, %(url_news)s) "
                      "ON DUPLICATE KEY UPDATE "
                      "adjusted_pt_current = VALUES(adjusted_pt_current), adjusted_pt_prior = VALUES(adjusted_pt_prior), "
                      "pt_current = VALUES(pt_current), pt_prior = VALUES(pt_prior), rating_current = VALUES(rating_current), "
                      "rating_prior = VALUES(rating_prior), updated = VALUES(updated)")
        
        for rating in rating_data:
            cursor.execute(add_rating, rating)
            added_ratings += 1

        return added_ratings

    except mysql.connector.Error as err:
        print(f"Error inserting rating data: {err}")
        return added_ratings

def fetch_ratings_for_january_2021(ticker, cursor):
    # Fetch ratings for January 2021
    params = {
        'company_tickers': ticker,
        'date_from': '2021-01-01',
        'date_to': '2021-01-31'
    }
    
    print(f"Fetching January 2021 ratings for {ticker}")
    try:
        rating_data = bz.ratings(**params)
        if 'ratings' in rating_data and rating_data['ratings']:
            added_ratings = insert_rating_data(rating_data['ratings'], cursor)
            print(f"Added {added_ratings} ratings for {ticker}")
        else:
            print(f"No ratings found for {ticker} in January 2021")
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")

def fetch_and_store_ratings(tickers):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        for ticker in tickers:
            fetch_ratings_for_january_2021(ticker, cursor)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error occurred: {e}")

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

# Script execution
try:
    fetch_and_store_ratings(sp500_tickers)
    exit_program()
except Exception as e:
    print(f"An error occurred: {e}")
    exit_program()
