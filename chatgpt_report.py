from openai import OpenAI
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

# Initialize OpenAI client
client = OpenAI(api_key=chatgpt_key)

# Establish MySQL connection with a dictionary cursor
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)  # Using dictionary cursor to access by field names
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Function to fetch price target from 'analysis_simulation' table
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

# Function to check if a report exists for the given ticker in the past week
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

# Generate full report using ChatGPT with structured data
def generate_full_report(ticker, price_target):
    prompt = f"""
    Generate a detailed 5-page stock performance analyst report for the company with the ticker {ticker}.
    Please provide the data in structured format for database extraction.

    1. **Executive Summary**: recommendation (e.g., Buy, Hold, Sell), price_target, key drivers, key risks.
    2. **Company Overview**: brief overview of the company's business, products, and market.
    3. **Financial Performance**: revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents.
    4. **Business Segments**: name, revenue, growth rate for key business segments.
    5. **Competitive Position**: competitors' names, market share, strengths, and weaknesses.
    6. **Valuation Metrics**: pe_ratio, ev_ebitda, price_sales_ratio.
    7. **Risk Factors**: List of risks facing the company.
    """
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content

# Centralized function to parse the structured data from ChatGPT into the correct sections
def parse_report(report):
    sections = {
        "executive_summary": {},
        "financial_performance": {},
        "business_segments": [],
        "competitive_position": [],
        "valuation_metrics": {},
        "risk_factors": []
    }
    
    lines = report.split("\n")
    
    for line in lines:
        line = line.strip()
        if "recommendation" in line:
            sections["executive_summary"]["recommendation"] = line.split(":")[1].strip()
        elif "price_target" in line:
            sections["executive_summary"]["price_target"] = float(line.split(":")[1].strip())
        elif "revenue_q3" in line:
            sections["financial_performance"]["revenue_q3"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
        elif "net_income_q3" in line:
            sections["financial_performance"]["net_income_q3"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
        elif "eps_q3" in line:
            sections["financial_performance"]["eps_q3"] = float(line.split(":")[1].strip())
        elif "gross_margin" in line:
            sections["financial_performance"]["gross_margin"] = float(line.split(":")[1].replace("%", "").strip())
        elif "operating_margin" in line:
            sections["financial_performance"]["operating_margin"] = float(line.split(":")[1].replace("%", "").strip())
        elif "cash_equivalents" in line:
            sections["financial_performance"]["cash_equivalents"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
        elif "segment_name" in line:
            segment_name = line.split(":")[1].strip()
            segment_revenue = float(lines[lines.index(line)+1].split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            segment_growth_rate = float(lines[lines.index(line)+2].split(":")[1].replace("%", "").strip())
            sections["business_segments"].append({
                "segment_name": segment_name,
                "segment_revenue": segment_revenue,
                "segment_growth_rate": segment_growth_rate
            })
        elif "competitor_name" in line:
            competitor_name = line.split(":")[1].strip()
            market_share = float(lines[lines.index(line)+1].split(":")[1].strip())
            strengths = lines[lines.index(line)+2].split(":")[1].strip()
            weaknesses = lines[lines.index(line)+3].split(":")[1].strip()
            sections["competitive_position"].append({
                "competitor_name": competitor_name,
                "market_share": market_share,
                "strengths": strengths,
                "weaknesses": weaknesses
            })
        elif "pe_ratio" in line:
            sections["valuation_metrics"]["pe_ratio"] = float(line.split(":")[1].strip())
        elif "ev_ebitda" in line:
            sections["valuation_metrics"]["ev_ebitda"] = float(line.split(":")[1].strip())
        elif "price_sales_ratio" in line:
            sections["valuation_metrics"]["price_sales_ratio"] = float(line.split(":")[1].strip())
        elif "risk" in line:
            sections["risk_factors"].append(line.split(":")[1].strip())

    return sections

# Function to insert parsed sections into the database
def insert_report_data(ticker, sections):
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    cursor.execute("SELECT stock_id FROM StockInformation WHERE ticker_symbol = %s", (ticker,))
    stock_info = cursor.fetchone()

    if not stock_info:
        print(f"Stock information not found for ticker {ticker}. Fetching it from ChatGPT API.")
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
    else:
        stock_id = stock_info['stock_id']

    # Insert into Reports table
    query_reports = "INSERT INTO Reports (stock_id, report_date) VALUES (%s, %s)"
    cursor.execute(query_reports, (stock_id, report_date))
    conn.commit()
    report_id = cursor.lastrowid

    # Insert financial performance
    financial_data = sections['financial_performance']
    query_financial = """
        INSERT INTO FinancialPerformance 
        (report_id, stock_id, revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query_financial, (
        report_id, stock_id, financial_data['revenue_q3'], financial_data['net_income_q3'], 
        financial_data['eps_q3'], financial_data['gross_margin'], 
        financial_data['operating_margin'], financial_data['cash_equivalents']
    ))

    # Insert business segments
    query_segments = """
        INSERT INTO BusinessSegments (report_id, stock_id, segment_name, segment_revenue, segment_growth_rate)
        VALUES (%s, %s, %s, %s, %s)
    """
    for segment in sections['business_segments']:
        cursor.execute(query_segments, (
            report_id, stock_id, segment['segment_name'], segment['segment_revenue'], segment['segment_growth_rate']
        ))

    # Insert competitive position
    query_competitors = """
        INSERT INTO CompetitivePosition (report_id, stock_id, competitor_name, market_share, strengths, weaknesses)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for competitor in sections['competitive_position']:
        cursor.execute(query_competitors, (
            report_id, stock_id, competitor['competitor_name'], competitor['market_share'], 
            competitor['strengths'], competitor['weaknesses']
        ))

    # Insert valuation metrics
    valuation_data = sections['valuation_metrics']
    query_valuation = """
        INSERT INTO ValuationMetrics (report_id, stock_id, pe_ratio, ev_ebitda, price_sales_ratio)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(query_valuation, (
        report_id, stock_id, valuation_data['pe_ratio'], valuation_data['ev_ebitda'], 
        valuation_data['price_sales_ratio']
    ))

    # Insert risk factors
    query_risks = "INSERT INTO RiskFactors (report_id, stock_id, risk) VALUES (%s, %s, %s)"
    for risk in sections['risk_factors']:
        cursor.execute(query_risks, (report_id, stock_id, risk))

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
            insert_report_data(ticker, sections)

            print(f"Processed {ticker}")
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
