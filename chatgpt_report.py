import openai
import os
import sys
import mysql.connector
from datetime import datetime, timedelta

# Define the list of tickers (Amazon, Adobe, Nvidia)
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

# Set the OpenAI API key
openai.api_key = chatgpt_key

# Establish MySQL connection
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Function to fetch expected return (price target) from 'analysis_simulation' table
def fetch_price_target(ticker):
    query = """
        SELECT avg_combined_criteria
        FROM analysis_simulation
        WHERE ticker = %s
        ORDER BY date DESC LIMIT 1
    """
    cursor.execute(query, (ticker,))
    result = cursor.fetchone()
    return result[0] if result else None

# Function to check if a report has been generated in the last week for the given ticker
def is_recent_entry(ticker):
    one_week_ago = datetime.now() - timedelta(weeks=1)

    # Query to check if a report for the given ticker has been generated within the last week
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
    
    # If a result is found, check if the last report date is within the last week
    if result:
        last_report_date = result[0]
        return last_report_date >= one_week_ago
    return False

# Function to generate the full 5-page report using ChatGPT
def generate_full_report(ticker, price_target):
    prompt = f"""
    Generate a detailed 5-page stock performance analyst report for the company with the ticker {ticker}.
    Here is what to include:

    1. **Executive Summary**:
       - Provide a recommendation for the stock (e.g., Buy, Hold, Sell)
       - Mention the price target: {price_target}
       - Provide a brief overview of key drivers and risks.

    2. **Company Overview**:
       - Describe the company's business, key products, and market positioning.

    3. **Financial Performance**:
       - Provide the latest financial data, including revenue, net income, EPS, and profit margins.

    4. **Business Segments**:
       - List the key business segments and their respective revenues and growth rates.

    5. **Valuation Metrics**:
       - Provide the P/E ratio, EV/EBITDA, and other relevant valuation ratios.

    6. **Risk Factors**:
       - List the major risks facing the company and the stock.

    Generate the report in sections with a clear and professional tone.
    """
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    full_report = response.choices[0]['message']['content']
    return full_report

# Function to parse the full report into individual sections (e.g., financial performance, business segments)
def parse_report_sections(full_report):
    sections = {
        'executive_summary': "",
        'company_overview': "",
        'financial_performance': "",
        'business_segments': "",
        'valuation_metrics': "",
        'risk_factors': ""
    }
    
    # Split the report by section headers and assign text to appropriate sections
    lines = full_report.split('\n')
    current_section = None
    
    for line in lines:
        if "Executive Summary" in line:
            current_section = 'executive_summary'
        elif "Company Overview" in line:
            current_section = 'company_overview'
        elif "Financial Performance" in line:
            current_section = 'financial_performance'
        elif "Business Segments" in line:
            current_section = 'business_segments'
        elif "Valuation Metrics" in line:
            current_section = 'valuation_metrics'
        elif "Risk Factors" in line:
            current_section = 'risk_factors'
        
        if current_section and line.strip() and current_section in sections:
            sections[current_section] += line.strip() + " "

    return sections

# Function to query ChatGPT API to get stock information
def fetch_stock_info_from_chatgpt(ticker):
    # Create a prompt to request stock information (e.g., name, sector, and exchange)
    prompt = f"Provide the full company name, sector, and stock exchange for the ticker symbol {ticker}."

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )

        stock_info = response['choices'][0]['message']['content']
        return stock_info
    except Exception as e:
        print(f"Error fetching stock information from ChatGPT for {ticker}: {e}")
        return None

# Function to insert parsed sections into the appropriate tables in the database
def insert_report_data(ticker, sections):
    # Get the current date and time
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Step 1: Retrieve the stock_id from StockInformation table or insert if it doesn't exist
    cursor.execute("SELECT stock_id FROM StockInformation WHERE ticker_symbol = %s", (ticker,))
    stock_info = cursor.fetchone()

    if not stock_info:
        print(f"Stock information not found for ticker {ticker}. Fetching it from ChatGPT API.")

        # Use ChatGPT to retrieve stock information
        stock_info_text = fetch_stock_info_from_chatgpt(ticker)
        if not stock_info_text:
            print(f"Failed to retrieve stock information for ticker {ticker}. Skipping.")
            return
        
        # Parsing the stock information from the ChatGPT response (custom parsing based on response format)
        # Example response: "The company name is Amazon.com Inc., the sector is Technology, and the exchange is NASDAQ."
        try:
            stock_name = stock_info_text.split("company name is")[1].split(",")[0].strip()
            sector = stock_info_text.split("sector is")[1].split(",")[0].strip()
            exchange = stock_info_text.split("exchange is")[1].split(".")[0].strip()
        except IndexError as e:
            print(f"Error parsing stock information from ChatGPT response: {e}")
            return

        # Insert the stock into StockInformation
        query_insert_stock = """
            INSERT INTO StockInformation (stock_name, ticker_symbol, sector, exchange) 
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query_insert_stock, (stock_name, ticker, sector, exchange))
        conn.commit()

        # Retrieve the newly inserted stock_id
        stock_id = cursor.lastrowid
    else:
        stock_id = stock_info['stock_id']

    # Step 2: Insert into Reports table
    query_reports = """
        INSERT INTO Reports (stock_id, report_date) 
        VALUES (%s, %s)
    """
    cursor.execute(query_reports, (stock_id, report_date))
    conn.commit()
    report_id = cursor.lastrowid  # Get the last inserted report_id

    # Step 3: Insert into FinancialPerformance table using extracted data
    financial_data = sections.get('financial_data', {})
    query_financial = """
        INSERT INTO FinancialPerformance 
        (report_id, stock_id, revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query_financial, (
        report_id, 
        stock_id, 
        financial_data.get('revenue_q3'), 
        financial_data.get('net_income_q3'), 
        financial_data.get('eps_q3'), 
        financial_data.get('gross_margin'), 
        financial_data.get('operating_margin'), 
        financial_data.get('cash_equivalents')
    ))

    # Step 4: Insert into BusinessSegments table using data from parsed sections
    business_segments = sections.get('business_segments', [])
    query_segments = """
        INSERT INTO BusinessSegments (report_id, stock_id, segment_name, segment_revenue, segment_growth_rate)
        VALUES (%s, %s, %s, %s, %s)
    """
    for segment in business_segments:
        cursor.execute(query_segments, (
            report_id, 
            stock_id, 
            segment.get('name'), 
            segment.get('revenue'), 
            segment.get('growth_rate')
        ))

    # Step 5: Insert into ValuationMetrics table using extracted data
    valuation_data = sections.get('valuation_metrics', {})
    query_valuation = """
        INSERT INTO ValuationMetrics (report_id, stock_id, pe_ratio, ev_ebitda, price_sales_ratio, valuation_method)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query_valuation, (
        report_id, 
        stock_id, 
        valuation_data.get('pe_ratio'), 
        valuation_data.get('ev_ebitda'), 
        valuation_data.get('price_sales_ratio'), 
        valuation_data.get('valuation_method')
    ))

    # Step 6: Insert into RiskFactors table using extracted data
    risk_factors = sections.get('risk_factors', [])
    query_risk = """
        INSERT INTO RiskFactors (report_id, stock_id, risk)
        VALUES (%s, %s, %s)
    """
    for risk in risk_factors:
        cursor.execute(query_risk, (
            report_id, 
            stock_id, 
            risk
        ))

    # Commit all the changes to the database
    conn.commit()
    print(f"Successfully inserted report data for {ticker} (Report ID: {report_id}).")

# Main script to process stock data for predefined tickers
def process_stocks(tickers):
    for ticker in tickers:
        try:
            # Step 1: Check if a recent recommendation exists
            if is_recent_entry(ticker):
                print(f"Recent recommendation exists for {ticker}, skipping.")
                continue

            # Step 2: Fetch the price target
            price_target = fetch_price_target(ticker)
            if not price_target:
                print(f"No price target found for {ticker}, skipping.")
                continue

            # Step 3: Generate the full 5-page report using ChatGPT
            full_report = generate_full_report(ticker, price_target)

            # Step 4: Parse the report into sections
            sections = parse_report_sections(full_report)

            # Step 5: Store the report data in the appropriate tables
            insert_report_data(ticker, sections)

            print(f"Processed {ticker}")

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

# Script execution
if __name__ == "__main__":
    try:
        process_stocks(tickers)  # Processing only the tickers defined at the start
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
