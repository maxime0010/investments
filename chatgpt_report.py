from openai import OpenAI
import os
import sys
import mysql.connector
from datetime import datetime, timedelta
import re

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

# Initialize OpenAI client
client = OpenAI(api_key=chatgpt_key)

# Establish MySQL connection with a dictionary cursor
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)  # Using dictionary cursor to access by field names
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
    return result['avg_combined_criteria'] if result else None

# Function to check if a report has been generated in the last week for the given ticker
def is_recent_entry(ticker):
    one_week_ago = (datetime.now() - timedelta(weeks=1)).date()  # Convert to date

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
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    full_report = response.choices[0].message.content
    return full_report


# Function to extract key financial metrics (e.g., revenue, net income, EPS) using regex
def extract_financial_data(financial_text):
    financial_data = {}

    # Use regular expressions to find the numbers in the financial text
    revenue_match = re.search(r'revenue of \$(\d+\.?\d*) billion', financial_text, re.IGNORECASE)
    net_income_match = re.search(r'net income of \$(\d+\.?\d*) billion', financial_text, re.IGNORECASE)
    eps_match = re.search(r'EPS of \$(\d+\.?\d*)', financial_text, re.IGNORECASE)
    gross_margin_match = re.search(r'gross margin was (\d+\.?\d*)%', financial_text, re.IGNORECASE)
    operating_margin_match = re.search(r'operating margin was (\d+\.?\d*)%', financial_text, re.IGNORECASE)
    cash_equivalents_match = re.search(r'cash equivalents of \$(\d+\.?\d*) billion', financial_text, re.IGNORECASE)

    # If the regex finds the number, convert it to the appropriate data type
    if revenue_match:
        financial_data['revenue_q3'] = float(revenue_match.group(1)) * 1e9  # Convert from billion to number
    if net_income_match:
        financial_data['net_income_q3'] = float(net_income_match.group(1)) * 1e9
    if eps_match:
        financial_data['eps_q3'] = float(eps_match.group(1))
    if gross_margin_match:
        financial_data['gross_margin'] = float(gross_margin_match.group(1))
    if operating_margin_match:
        financial_data['operating_margin'] = float(operating_margin_match.group(1))
    if cash_equivalents_match:
        financial_data['cash_equivalents'] = float(cash_equivalents_match.group(1)) * 1e9

    return financial_data


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

    # Log the financial performance section for debugging
    print(f"Financial Performance Section: {sections['financial_performance']}")
    
    return sections

# Function to query ChatGPT API to get stock information
def fetch_stock_info_from_chatgpt(ticker):
    # Create a prompt to request stock information (e.g., name, sector, and exchange)
    prompt = f"Provide the full company name, sector, and stock exchange for the ticker symbol {ticker}."

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}]
        )

        stock_info = response.choices[0].message.content
        print(f"Response from ChatGPT for {ticker}: {stock_info}")

        return stock_info
    except Exception as e:
        print(f"Error fetching stock information from ChatGPT for {ticker}: {e}")
        return None


# Updated parsing for stock information
def insert_report_data(ticker, sections):
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
        
        lines = stock_info_text.split('\n')
        stock_name, sector, exchange = "Unknown Company", "Unknown Sector", "Unknown Exchange"

        for line in lines:
            if "Company Name:" in line:
                stock_name = line.split("Company Name:")[1].strip()
            elif "Sector:" in line:
                sector = line.split("Sector:")[1].strip()
            elif "Stock Exchange:" in line:
                exchange = line.split("Stock Exchange:")[1].strip()

        print(f"Parsed stock name: {stock_name}, sector: {sector}, exchange: {exchange}")

        query_insert_stock = """
            INSERT INTO StockInformation (stock_name, ticker_symbol, sector, exchange) 
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query_insert_stock, (stock_name, ticker, sector, exchange))
        conn.commit()

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

    # Step 3: Extract financial data
    financial_data = extract_financial_data(sections['financial_performance'])
    
    if not financial_data:
        print(f"Missing financial data for {ticker}, skipping.")
        return
    
    # Step 4: Insert into FinancialPerformance table using extracted data
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

    # Insert other sections like BusinessSegments, ValuationMetrics, RiskFactors...
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
