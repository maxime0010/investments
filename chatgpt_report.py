import os
import sys
import mysql.connector
from openai import OpenAI
from datetime import datetime, timedelta
import json

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

def generate_full_report(ticker, price_target):
    prompt = f"""
    Generate a detailed stock performance analyst report for the company with the ticker {ticker}.
    Please provide the entire response **strictly in JSON format** without any additional comments or explanations. 
    The JSON structure must be as follows:

    {{
        "Executive_Summary": {{
            "recommendation": "Buy, Hold, or Sell",
            "price_target": "{price_target}",  # Use this input value, do not generate it
            "key_drivers": ["Main driver 1", "Main driver 2", "Main driver 3"],
            "key_risks": ["Main risk 1", "Main risk 2", "Main risk 3"]
        }},
        "Company_Overview": {{
            "description": "A brief description of the company",
            "products": ["Product 1", "Product 2", "Product 3"],
            "market": "The main markets where the company operates"
        }},
        "Financial_Performance": {{
            "revenue_q3": "Revenue in dollars (e.g., $100.5 billion)",
            "net_income_q3": "Net income in dollars (e.g., $10.2 billion)",
            "eps_q3": "EPS value",
            "gross_margin": "Gross margin in percentage (e.g., 40%)",
            "operating_margin": "Operating margin in percentage (e.g., 20%)",
            "cash_equivalents": "Cash and equivalents in dollars (e.g., $50.5 billion)"
        }},
        "Business_Segments": [
            {{
                "name": "Segment 1",
                "revenue": "Revenue in dollars (e.g., $10.5 billion)",
                "growth_rate": "Growth rate in percentage (e.g., 15%)"
            }},
            {{
                "name": "Segment 2",
                "revenue": "Revenue in dollars (e.g., $8.5 billion)",
                "growth_rate": "Growth rate in percentage (e.g., 10%)"
            }}
        ],
        "Competitive_Position": {{
            "competitors": ["Competitor 1", "Competitor 2"],
            "market_share": "Market share in percentage (e.g., 40%)",
            "strengths": ["Strength 1", "Strength 2"],
            "weaknesses": ["Weakness 1", "Weakness 2"]
        }},
        "Valuation_Metrics": {{
            "pe_ratio": "P/E ratio value",
            "ev_ebitda": "EV/EBITDA ratio value",
            "price_sales_ratio": "Price-to-sales ratio value"
        }},
        "Risk_Factors": [
            "Risk factor 1",
            "Risk factor 2",
            "Risk factor 3"
        ]
    }}
    """
    
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content



def parse_report(report):
    try:
        parsed_data = json.loads(report)  # Directly load the JSON
        print(f"Parsed Report Data: {parsed_data}")
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse the report as JSON. {e}")
        return None



def insert_report_data(ticker, sections):
    print(f"Inserting report data for {ticker}")
    report_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get stock_id or insert new stock information
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

    # Insert financial performance data
    financial_data = sections.get('Financial_Performance', {})
    if financial_data:
        query_financial = """
            INSERT INTO FinancialPerformance 
            (report_id, stock_id, revenue_q3, net_income_q3, eps_q3, gross_margin, operating_margin, cash_equivalents)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query_financial, (
            report_id, stock_id, 
            financial_data.get('revenue_q3').replace('$', '').replace(',', ''), 
            financial_data.get('net_income_q3').replace('$', '').replace(',', ''), 
            financial_data.get('eps_q3'), 
            financial_data.get('gross_margin').replace('%', ''), 
            financial_data.get('operating_margin').replace('%', ''), 
            financial_data.get('cash_equivalents').replace('$', '').replace(',', '')
        ))
        print(f"Inserted financial performance for {ticker}")
    else:
        print(f"Financial data missing for {ticker}, skipping.")

    # Insert business segments data
    business_segments = sections.get('Business_Segments', [])
    if business_segments:
        query_segments = """
            INSERT INTO BusinessSegments (report_id, stock_id, segment_name, segment_revenue, segment_growth_rate)
            VALUES (%s, %s, %s, %s, %s)
        """
        for segment in business_segments:
            cursor.execute(query_segments, (
                report_id, stock_id, 
                segment['name'], 
                segment['revenue'].replace('$', '').replace(',', ''), 
                segment['growth_rate'].replace('%', '')
            ))
        print(f"Inserted business segments for {ticker}")
    else:
        print(f"No business segments data for {ticker}, skipping.")

    # Insert competitive position data
    competitive_position = sections.get('Competitive_Position', {})
    if competitive_position:
        query_competitors = """
            INSERT INTO CompetitivePosition (report_id, stock_id, competitor_name, market_share, strengths, weaknesses)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query_competitors, (
            report_id, stock_id, ', '.join(competitive_position.get('competitors', [])), 
            competitive_position.get('market_share').replace('%', ''), 
            ', '.join(competitive_position.get('strengths', [])), 
            ', '.join(competitive_position.get('weaknesses', []))
        ))
        print(f"Inserted competitive position data for {ticker}")
    else:
        print(f"No competitive position data for {ticker}, skipping.")

    # Insert valuation metrics data
    valuation_data = sections.get('Valuation_Metrics', {})
    if valuation_data:
        query_valuation = """
            INSERT INTO ValuationMetrics (report_id, stock_id, pe_ratio, ev_ebitda, price_sales_ratio)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query_valuation, (
            report_id, stock_id, 
            valuation_data['pe_ratio'], 
            valuation_data['ev_ebitda'], 
            valuation_data['price_sales_ratio']
        ))
        print(f"Inserted valuation metrics for {ticker}")
    else:
        print(f"No valuation metrics data for {ticker}, skipping.")

    # Insert risk factors data
    risk_factors = sections.get('Risk_Factors', [])
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
