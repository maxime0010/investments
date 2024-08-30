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

    # Fetch the overall_success_rate column and order it
    cursor.execute("SELECT overall_success_rate FROM analysts ORDER BY overall_success_rate")
    success_rates = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    # Calculate the median
    n = len(success_rates)
    if n == 0:
        return 0  # Return 0 or a default value if there are no records

    if n % 2 == 1:
        return success_rates[n // 2]
    else:
        mid_index = n // 2
        return (success_rates[mid_index - 1] + success_rates[mid_index]) / 2

# Configuration settings
DAYS_RECENT = 30  # Number of days to define "recent"
SUCCESS_RATE_THRESHOLD = calculate_median_success_rate()  # Dynamically calculated
MIN_ANALYSTS = 3  # Minimum number of analysts covering a stock
