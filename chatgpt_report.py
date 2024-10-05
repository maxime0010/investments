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
    Please provide the data in a structured format for easy extraction into a database.

    Here is what to include:

    1. **Executive Summary**:
       recommendation: (e.g., Buy, Hold, Sell)
       price_target: {price_target}
       key_drivers: [List of key drivers]
       key_risks: [List of key risks]

    2. **Company Overview**:
       business_overview: (Brief description of the company’s business, products, and market positioning)

    3. **Financial Performance** (structured data):
       revenue_q3: $X.X billion
       net_income_q3: $X.X billion
       eps_q3: $X.XX
       gross_margin: X.X%
       operating_margin: X.X%
       cash_equivalents: $X.X billion

    4. **Business Segments**:
       segments: [
           {
               "segment_name": "Segment 1",
               "segment_revenue": $X.X billion,
               "segment_growth_rate": X.X%
           },
           {
               "segment_name": "Segment 2",
               "segment_revenue": $X.X billion,
               "segment_growth_rate": X.X%
           }
       ]

    5. **Competitive Position**:
       competitors: [
           {
               "competitor_name": "Competitor 1",
               "market_share": X.X,
               "strengths": "Strengths of competitor",
               "weaknesses": "Weaknesses of competitor"
           }
       ]

    6. **Valuation Metrics**:
       pe_ratio: X.X
       ev_ebitda: X.X
       price_sales_ratio: X.X

    7. **Risk Factors**:
       risks: [List of major risks]

    Generate the report in sections with a clear and structured format.
    """
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    full_report = response.choices[0].message.content
    return full_report

# Function to parse the structured financial data and other sections
def parse_report_sections(full_report):
    sections = {}

    lines = full_report.split('\n')

    # Parse each section into the dictionary (adjust according to the structure of the report)
    for line in lines:
        line = line.strip()

        if line.startswith("recommendation"):
            sections['executive_summary'] = {
                'recommendation': line.split(":")[1].strip(),
                'key_drivers': [],
                'key_risks': []
            }
        elif line.startswith("business_overview"):
            sections['company_overview'] = line.split(":")[1].strip()
        elif line.startswith("revenue_q3"):
            sections['financial_performance'] = {
                'revenue_q3': float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            }
        elif line.startswith("net_income_q3"):
            sections['financial_performance']['net_income_q3'] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
        elif line.startswith("eps_q3"):
            sections['financial_performance']['eps_q3'] = float(line.split(":")[1].strip())
        elif line.startswith("gross_margin"):
            sections['financial_performance']['gross_margin'] = float(line.split(":")[1].replace("%", "").strip())
        elif line.startswith("operating_margin"):
            sections['financial_performance']['operating_margin'] = float(line.split(":")[1].replace("%", "").strip())
        elif line.startswith("cash_equivalents"):
            sections['financial_performance']['cash_equivalents'] = float(line.split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
        elif line.startswith("segments"):
            sections['business_segments'] = []
        elif "segment_name" in line:
            segment_name = line.split(":")[1].strip()
            segment_revenue = float(lines[lines.index(line)+1].split(":")[1].replace("$", "").replace("billion", "").strip()) * 1e9
            segment_growth_rate = float(lines[lines.index(line)+2].split(":")[1].replace("%", "").strip())
            sections['business_segments'].append({
                'segment_name': segment_name,
                'segment_revenue': segment_revenue,
                'segment_growth_rate': segment_growth_rate
            })
        elif line.startswith("pe_ratio"):
            sections['valuation_metrics'] = {
                'pe_ratio': float(line.split(":")[1].strip())
            }
        elif line.startswith("ev_ebitda"):
            sections['valuation_metrics']['ev_ebitda'] = float(line.split(":")[1].strip())
        elif line.startswith("price_sales_ratio"):
            sections['valuation_metrics']['price_sales_ratio'] = float(line.split(":")[1].strip())
        elif line.startswith("competitors"):
            sections['competitive_position'] = []
        elif "competitor_name" in line:
            competitor_name = line.split(":")[1].strip()
            market_share = float(lines[lines.index(line)+1].split(":")[1].strip())
            strengths = lines[lines.index(line)+2].split(":")[1].strip()
            weaknesses = lines[lines.index(line)+3].split(":")[1].strip()
            sections['competitive_position'].append({
                'competitor_name': competitor_name,
                'market_share': market_share,
                'strengths': strengths,
                'weaknesses': weaknesses
            })
        elif line.startswith("risks"):
            sections['risk_factors'] = []

    return sections

# Insert parsed sections into the database
def insert_report_data(ticker, sections):
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Step 1: Retrieve the stock_id from StockInformation table or insert if it doesn't exist
    cursor.execute("SELECT stock_id FROM StockInformation WHERE ticker_symbol = %s", (ticker,))
    stock_info = cursor.fetchone()

    if not stock_info:
        print(f"Stock information not found for ticker {ticker}. Fetching it from ChatGPT API.")

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
    financial_data = sections.get('financial_performance', {})
    
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

    # Step 5: Insert into BusinessSegments table using extracted data
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

    # Step 6: Insert into CompetitivePosition table
    competitive_position = sections.get('competitive_position', [])
    query_competition = """
        INSERT INTO CompetitivePosition (report_id, stock_id, competitor_name, market_share, strengths, weaknesses)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for competitor in competitive_position:
        cursor.execute(query_competition, (
            report_id,
            stock_id,
            competitor.get('competitor_name'),
            competitor.get('market_share'),
            competitor.get('strengths'),
            competitor.get('weaknesses')
        ))

    # Step 7: Insert into ValuationMetrics table
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
        "P/E multiples based on future earnings"
    ))

    # Step 8: Insert into RiskFactors table
    risk_factors = sections.get('risk_factors', [])
    query_risks = """
        INSERT INTO RiskFactors (report_id, stock_id, risk)
        VALUES (%s, %s, %s)
    """
    for risk in risk_factors:
        cursor.execute(query_risks, (
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
