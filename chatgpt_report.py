import os
import sys
import mysql.connector
import pdfplumber
from openai import OpenAI
from datetime import datetime
import json
import shutil

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
    cursor = conn.cursor(dictionary=True)
    print("Successfully connected to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
    sys.exit()

# Create a new table for storing the generated report
def create_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS InvestmentReports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATETIME,
        ticker VARCHAR(10),
        dimension VARCHAR(50),
        question TEXT,
        answer TEXT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

# Function to check for files in ./files directory and subfolders
def find_files_in_directory():
    files_to_process = []
    base_dir = './files'  # Use relative path to the folder 'files'
    for subdir, dirs, files in os.walk(base_dir):
        print(f"Checking directory: {subdir}")  # Debugging log to see which directories are being checked
        for file in files:
            print(f"Found file: {file} in {subdir}")  # Debugging log to see found files
            file_path = os.path.join(subdir, file)
            ticker = os.path.basename(subdir)  # Subfolder name is the ticker
            files_to_process.append((file_path, ticker))
    return files_to_process



# Function to extract text from a PDF file
def extract_text_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n"
        return text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return None

# Function to send the file (PDF text) to ChatGPT API and generate questions and answers
def generate_questions_from_report(file_path, ticker):
    if file_path.lower().endswith('.pdf'):
        # Extract text from the PDF
        report_content = extract_text_from_pdf(file_path)
        if report_content is None:
            print(f"Failed to extract text from {file_path}")
            return None
    else:
        # For other formats (if needed), you can extend here
        with open(file_path, 'r') as report_file:
            report_content = report_file.read()

    prompt = f"""
    As an investment analyst, could you generate 20 questions and answers based on this annual report? It should support decision to invest or not, be strategic and business oriented. Please structure around 4 categories: strengths, weaknesses, opportunities, threats. 5 questions and answers in each category. You are an analyst, so the questions and answers should extract deep insights for the reader and help him decide if he should invest or not. Each answer should have approximately 500 words and be structured and argumented.

    Note the ticker is {ticker}.

    Please structure the output as a JSON as it will be used as an input for a python request to insert in a database.
    """

    # Send the extracted report content to the ChatGPT API
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content


# Function to parse the response into a JSON structure
def parse_json_response(response):
    try:
        return json.loads(response)
    except json.JSONDecodeError as e:
        print(f"Failed to parse response JSON: {e}")
        return None

# Function to insert the report into the database
def insert_report_into_db(ticker, json_data):
    for dimension in ['Strengths', 'Weaknesses', 'Opportunities', 'Threats']:
        for entry in json_data[dimension]:
            question = entry['question']
            answer = entry['answer']
            date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            query = """
            INSERT INTO InvestmentReports (date, ticker, dimension, question, answer)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (date_now, ticker, dimension, question, answer))
    
    conn.commit()
    print(f"Inserted data for {ticker} into InvestmentReports")

# Function to move processed files to /processed directory
def move_processed_file(file_path):
    processed_dir = '/processed'
    os.makedirs(processed_dir, exist_ok=True)
    shutil.move(file_path, os.path.join(processed_dir, os.path.basename(file_path)))
    print(f"Moved {file_path} to {processed_dir}")

# Main process function
def process_files():
    create_table()
    files_to_process = find_files_in_directory()

    if not files_to_process:
        print("No files found in /files directory.")
        return

    for file_path, ticker in files_to_process:
        print(f"Processing file: {file_path} for ticker: {ticker}")
        try:
            # Generate Q&A from the report
            report_json_response = generate_questions_from_report(file_path, ticker)
            json_data = parse_json_response(report_json_response)

            if json_data:
                # Insert the JSON data into the database
                insert_report_into_db(ticker, json_data)

                # Move file to processed directory
                move_processed_file(file_path)
            else:
                print(f"Failed to generate or parse report for {ticker}. Skipping.")
        except Exception as e:
            print(f"Error processing file {file_path} for ticker {ticker}: {e}")

if __name__ == "__main__":
    try:
        process_files()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
