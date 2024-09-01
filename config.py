import mysql.connector
import os

# Retrieve MySQL password and host from environment variables
mdp = os.getenv("MYSQL_MDP")
if not mdp:
    raise ValueError("No MySQL password found in environment variables")
host = os.getenv("MYSQL_HOST")
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

def calculate_median_success_rate():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Calculate median overall_success_rate
    query = """
        SELECT 
            ROUND(AVG(t.overall_success_rate), 2) AS median_success_rate
        FROM (
            SELECT 
                overall_success_rate,
                @rownum := @rownum + 1 AS row_number,
                @total_rows := @rownum
            FROM 
                analysts, (SELECT @rownum := 0) r
            ORDER BY 
                overall_success_rate
        ) AS t
        WHERE 
            t.row_number IN (FLOOR((@total_rows + 1) / 2), CEIL((@total_rows + 1) / 2));
    """
    cursor.execute(query)
    median_success_rate = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return median_success_rate


# Configuration settings
DAYS_RECENT = 31  # Number of days to define "recent"
SUCCESS_RATE_THRESHOLD = calculate_median_success_rate()  # Dynamically calculated
MIN_ANALYSTS = 3  # Minimum number of analysts covering a stock
