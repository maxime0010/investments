import mysql.connector
import os
from decimal import Decimal

# Database connection configuration
db_config = {
    'user': 'doadmin',
    'password': os.getenv("MYSQL_MDP"),
    'host': os.getenv("MYSQL_HOST"),
    'database': 'defaultdb',
    'port': 25060
}

def calculate_median_success_rate():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Calculate median overall_success_rate using ROW_NUMBER() and CTEs (Common Table Expressions)
    query = """
        WITH ordered_analysts AS (
            SELECT overall_success_rate, ROW_NUMBER() OVER (ORDER BY overall_success_rate) AS rn, COUNT(*) OVER() AS cnt
            FROM analysts
        )
        SELECT ROUND(AVG(overall_success_rate), 2) AS median_success_rate
        FROM ordered_analysts
        WHERE rn IN (FLOOR((cnt + 1) / 2), CEIL((cnt + 1) / 2));
    """
    cursor.execute(query)
    median_success_rate = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return Decimal(median_success_rate) if median_success_rate is not None else Decimal(0)

DAYS_RECENT = 30  # Number of days to define "recent"
SUCCESS_RATE_THRESHOLD = calculate_median_success_rate()  # Dynamically calculated
MIN_ANALYSTS = 3  # Minimum number of analysts covering a stock
MAX_STDDEV = 100

