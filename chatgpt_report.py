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
    print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Function to fetch price target from 'analysis_simulation' table
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
    if result:
        print(f"Price target for {ticker}: {result['avg_combined_criteria']}")
        return result['avg_combined_criteria']
    else:
        print(f"No price target found for {ticker}")
        return None

# Function to check if a report exists for the given ticker in the past week
def is_recent_entry(ticker):
    print(f"Checking for recent report for {ticker}")
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
        print(f"Last report date for {ticker}: {last_report_date}")
        return last_report_date >= one_week_ago
    else:
        print(f"No recent report found for {ticker}")
        return False

# Generate full report using ChatGPT with structured data
def generate_full_report(ticker, price_target):
    print(f"Generating report for {ticker} with price target: {price_target}")
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
    
    full_report = response.choices[0].message.content
    print(f"Generated report for {ticker}: \n{full_report}")
    return full_report

# Centralized function to parse the structured data from ChatGPT into the correct sections
def parse_report(report):
    print("Parsing report")
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
            try:
                sections["financial_performance"]["revenue_q3"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            except Exception as e:
                print(f"Error parsing revenue_q3: {e}")
        elif "net_income_q3" in line:
            try:
                sections["financial_performance"]["net_income_q3"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            except Exception as e:
                print(f"Error parsing net_income_q3: {e}")
        elif "eps_q3" in line:
            try:
                sections["financial_performance"]["eps_q3"] = float(line.split(":")[1].strip())
            except Exception as e:
                print(f"Error parsing eps_q3: {e}")
        elif "gross_margin" in line:
            try:
                sections["financial_performance"]["gross_margin"] = float(line.split(":")[1].replace("%", "").strip())
            except Exception as e:
                print(f"Error parsing gross_margin: {e}")
        elif "operating_margin" in line:
            try:
                sections["financial_performance"]["operating_margin"] = float(line.split(":")[1].replace("%", "").strip())
            except Exception as e:
                print(f"Error parsing operating_margin: {e}")
        elif "cash_equivalents" in line:
            try:
                sections["financial_performance"]["cash_equivalents"] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            except Exception as e:
                print(f"Error parsing cash_equivalents: {e}")
        elif "segment_name" in line:
            try:
                segment_name = line.split(":")[1].strip()
                segment_revenue = float(lines[lines.index(line)+1].split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
                segment_growth_rate = float(lines[lines.index(line)+2].split(":")[1].replace("%", "").strip())
                sections["business_segments"].append({
                    "segment_name": segment_name,
                    "segment_revenue": segment_revenue,
                    "segment_growth_rate": segment_growth_rate
                })
            except Exception as e:
                print(f"Error parsing business segments: {e}")
        elif "competitor_name" in line:
            try:
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
            except Exception as e:
                print(f"Error parsing competitive position: {e}")
        elif "pe_ratio" in line:
            try:
                sections["valuation_metrics"]["pe_ratio"] = float(line.split(":")[1].strip())
            except Exception as e:
                print(f"Error parsing pe_ratio: {e}")
        elif "ev_ebitda" in line:
            try:
                sections["valuation_metrics"]["ev_ebitda"] = float(line.split(":")[1].strip())
            except Exception as e:
                print(f"Error parsing ev_ebitda: {e}")
        elif "price_sales_ratio" in line:
            try:
                sections["valuation_metrics"]["price_sales_ratio"] = float(line.split(":")[1].strip())
            except Exception as e:
                print(f"Error parsing price_sales_ratio: {e}")
        elif "risk" in line:
            try:
                sections["risk_factors"].append(line.split(":")[1].strip())
            except Exception as e:
                print(f"Error parsing risk factors: {e}")

    # Debugging: Print parsed sections for verification
    print(f"Parsed Sections: {sections}")

    return sections

# Function to insert parsed sections into the database
def insert_report_data(ticker, sections):
    print(f"Inserting report data for {ticker}")
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

    # Insert financial performance data (check for existence of data)
    financial_data = sections.get('financial_performance', {})
    if not financial_data or 'revenue_q3' not in financial_data:
        print(f"Error: Missing financial data for {ticker}. Skipping financial performance insertion.")
    else:
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

    # Insert business segments data
    query_segments = """
        INSERT INTO BusinessSegments (report_id, stock_id, segment_name, segment_revenue, segment_growth_rate)
        VALUES (%s, %s, %s, %s, %s)
    """
    if sections['business_segments']:
        for segment in sections['business_segments']:
            cursor.execute(query_segments, (
                report_id, stock_id, segment['segment_name'], segment['segment_revenue'], segment['segment_growth_rate']
            ))
        print(f"Inserted business segments for {ticker}")
    else:
        print(f"No business segments data for {ticker}")

    # Insert competitive position data
    query_competitors = """
        INSERT INTO CompetitivePosition (report_id, stock_id, competitor_name, market_share, strengths, weaknesses)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    if sections['competitive_position']:
        for competitor in sections['competitive_position']:
            cursor.execute(query_competitors, (
                report_id, stock_id, competitor['competitor_name'], competitor['market_share'], 
                competitor['strengths'], competitor['weaknesses']
            ))
        print(f"Inserted competitive position data for {ticker}")
    else:
        print(f"No competitive position data for {ticker}")

    # Insert valuation metrics data
    valuation_data = sections['valuation_metrics']
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
        print(f"No valuation metrics data for {ticker}")

    # Insert risk factors data
    query_risks = "INSERT INTO RiskFactors (report_id, stock_id, risk) VALUES (%s, %s, %s)"
    if sections['risk_factors']:
        for risk in sections['risk_factors']:
            cursor.execute(query_risks, (report_id, stock_id, risk))
        print(f"Inserted risk factors for {ticker}")
    else:
        print(f"No risk factors data for {ticker}")

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
