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
        SELECT ROUND(AVG(sub.overall_success_rate), 2) AS median_success_rate
        FROM (
            SELECT overall_success_rate
            FROM analysts
            ORDER BY overall_success_rate
            LIMIT 2 - (SELECT COUNT(*) FROM analysts) % 2    -- odd or even number of rows
            OFFSET (SELECT FLOOR((COUNT(*) - 1) / 2) FROM analysts)
        ) AS sub;
    """
    cursor.execute(query)
    median_success_rate = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return Decimal(median_success_rate) if median_success_rate is not None else Decimal(0)



# Configuration settings
DAYS_RECENT = 31  # Number of days to define "recent"
SUCCESS_RATE_THRESHOLD = calculate_median_success_rate()  # Dynamically calculated
MIN_ANALYSTS = 3  # Minimum number of analysts covering a stock
