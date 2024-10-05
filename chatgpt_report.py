import os
import sys
import mysql.connector
from openai import OpenAI
from datetime import datetime, timedelta

# Define the list of tickers
tickers = ['AMZN', 'ADBE', 'NVDA']

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
client = OpenAI(api_key=chatgpt_key)

# Establish MySQL connection
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)  # Using dictionary cursor to access by field names
    print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Function to fetch price target
def fetch_price_target(ticker):
    print(f"Fetching price target for {ticker}")
    query = """
        SELECT avg_combined_criteria
        FROM analysis_simulation
        WHERE ticker = %s
        ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    return result['avg_combined_criteria'] if result else None

# Function to check if a recent report exists
def is_recent_entry(ticker):
    one_week_ago = (datetime.now() - timedelta(weeks=1)).date()

    query = """
        SELECT r.report_date
        FROM Reports r
        JOIN StockInformation s ON r.stock_id = s.stock_id
        WHERE s.ticker_symbol = %s
        ORDER BY r.report_date DESC
        LIMIT 1
    """
    
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()

    if result:
        last_report_date = result['report_date']
        return last_report_date >= one_week_ago
    return False

# Generate structured report using ChatGPT
def generate_full_report(ticker, price_target):
    prompt = f"""
    Generate a detailed 5-page stock performance analyst report for the company with the ticker {ticker}.
    Provide the data in structured format (JSON-like), including:
    
    1. Executive Summary: recommendation (e.g., Buy, Hold, Sell), price_target, key_drivers, key_risks.
    2. Company Overview: description, products, market.
    3. Financial Performance: revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents.
    4. Business Segments: name, revenue, growth rate for key segments.
    5. Competitive Position: competitors, market share, strengths, weaknesses.
    6. Valuation Metrics: pe_ratio, ev_ebitda, price_sales_ratio.
    7. Risk Factors: List of risks.
    """
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content

# Parse the structured report into a dictionary format
def parse_report(report):
    # Assuming the ChatGPT response is structured and JSON-like, directly parse it
    import json
    try:
        parsed_data = json.loads(report)
        print(f"Parsed Report Data: {parsed_data}")
        return parsed_data
    except json.JSONDecodeError:
        print("Error: Failed to parse the report as JSON. Check the structure of the report.")
        return None

# Insert parsed sections into the database
def insert_report_data(ticker, sections):
    print(f"Inserting report data for {ticker}")
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    cursor.execute("SELECT stock_id FROM StockInformation WHERE ticker_symbol = %s", (ticker,))
    stock_info = cursor.fetchone()

    if not stock_info:
        print(f"Stock information not found for {ticker}. Fetching it from ChatGPT API.")
        stock_info_text = fetch_stock_info_from_chatgpt(ticker)
        lines = stock_info_text.split('\n')
        stock_name = next(line.split(":")[1].strip() for line in lines if "Company Name" in line)
        sector = next(line.split(":")[1].strip() for line in lines if "Sector" in line)
        exchange = next(line.split(":")[1].strip() for line in lines if "Stock Exchange" in line)
        query_insert_stock = """
            INSERT INTO StockInformation (stock_name, ticker_symbol, sector, exchange) 
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query_insert_stock, (stock_name, ticker, sector, exchange))
        conn.commit()
        stock_id = cursor.lastrowid
        print(f"Inserted new stock information for {ticker} with stock_id {stock_id}")
    else:
        stock_id = stock_info['stock_id']
        print(f"Found existing stock information for {ticker} with stock_id {stock_id}")

    # Insert into Reports table
    query_reports = "INSERT INTO Reports (stock_id, report_date) VALUES (%s, %s)"
    cursor.execute(query_reports, (stock_id, report_date))
    conn.commit()
    report_id = cursor.lastrowid
    print(f"Inserted report for {ticker} with report_id {report_id}")

    # Insert financial performance data
    financial_data = sections.get('financial_performance', {})
    if financial_data:
        query_financial = """
            INSERT INTO FinancialPerformance 
            (report_id, stock_id, revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query_financial, (
            report_id, stock_id, financial_data.get('revenue_q3'), financial_data.get('net_income_q3'), 
            financial_data.get('eps_q3'), financial_data.get('gross_margin'), 
            financial_data.get('operating_margin'), financial_data.get('cash_equivalents')
        ))
        print(f"Inserted financial performance for {ticker}")
    else:
        print(f"Financial data missing for {ticker}, skipping.")

    # Insert business segments data
    business_segments = sections.get('business_segments', [])
    if business_segments:
        query_segments = """
            INSERT INTO BusinessSegments (report_id, stock_id, segment_name, segment_revenue, segment_growth_rate)
            VALUES (%s, %s, %s, %s, %s)
        """
        for segment in business_segments:
            cursor.execute(query_segments, (
                report_id, stock_id, segment['segment_name'], segment['segment_revenue'], segment['segment_growth_rate']
            ))
        print(f"Inserted business segments for {ticker}")
    else:
        print(f"No business segments data for {ticker}, skipping.")

    # Insert competitive position data
    competitive_position = sections.get('competitive_position', [])
    if competitive_position:
        query_competitors = """
            INSERT INTO CompetitivePosition (report_id, stock_id, competitor_name, market_share, strengths, weaknesses)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        for competitor in competitive_position:
            cursor.execute(query_competitors, (
                report_id, stock_id, competitor['competitor_name'], competitor['market_share'], 
                competitor['strengths'], competitor['weaknesses']
            ))
        print(f"Inserted competitive position data for {ticker}")
    else:
        print(f"No competitive position data for {ticker}, skipping.")

    # Insert valuation metrics data
    valuation_data = sections.get('valuation_metrics', {})
    if valuation_data:
        query_valuation = """
            INSERT INTO ValuationMetrics (report_id, stock_id, pe_ratio, ev_ebitda, price_sales_ratio)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query_valuation, (
            report_id, stock_id, valuation_data['pe_ratio'], valuation_data['ev_ebitda'], 
            valuation_data['price_sales_ratio']
        ))
        print(f"Inserted valuation metrics for {ticker}")
    else:
        print(f"No valuation metrics data for {ticker}, skipping.")

    # Insert risk factors data
    risk_factors = sections.get('risk_factors', [])
    if risk_factors:
        query_risks = "INSERT INTO RiskFactors (report_id, stock_id, risk) VALUES (%s, %s, %s)"
        for risk in risk_factors:
            cursor.execute(query_risks, (report_id, stock_id, risk))
        print(f"Inserted risk factors for {ticker}")
    else:
        print(f"No risk factors data for {ticker}, skipping.")

    conn.commit()
    print(f"Successfully inserted report data for {ticker} (Report ID: {report_id}).")

# Main script to process stock data for predefined tickers
def process_stocks(tickers):
    for ticker in tickers:
        try:
            if is_recent_entry(ticker):
                print(f"Recent recommendation exists for {ticker}, skipping.")
                continue

            price_target = fetch_price_target(ticker)
            if not price_target:
                print(f"No price target found for {ticker}, skipping.")
                continue

            full_report = generate_full_report(ticker, price_target)
            sections = parse_report(full_report)

            if sections:
                insert_report_data(ticker, sections)
                print(f"Processed {ticker}")
            else:
                print(f"Failed to parse report for {ticker}, skipping.")
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

if __name__ == "__main__":
    try:
        process_stocks(tickers)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
