def calculate_price_target_statistics(cursor):
    query = """
        SELECT 
            r.ticker,
            AVG(r.adjusted_pt_current) AS average_price_target,
            STDDEV(r.adjusted_pt_current) AS stddev_price_target,
            COUNT(DISTINCT r.analyst_name) AS num_analysts,
            MAX(r.date) AS last_update_date,
            AVG(DATEDIFF(NOW(), r.date)) AS avg_days_since_last_update,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.analyst_name END) AS num_analysts_last_7_days,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.adjusted_pt_current END) AS stddev_price_target_last_7_days,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN r.adjusted_pt_current END) AS average_price_target_last_7_days,
            COUNT(DISTINCT CASE WHEN a.overall_success_rate > 50 THEN r.analyst_name END) AS num_high_success_analysts,
            STDDEV(CASE WHEN a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS stddev_high_success_analysts,
            AVG(CASE WHEN a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS avg_high_success_analysts,
            COUNT(DISTINCT CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.analyst_name END) AS num_combined_criteria,
            STDDEV(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS stddev_combined_criteria,
            AVG(CASE WHEN r.date >= DATE_SUB(NOW(), INTERVAL 7 DAY) AND a.overall_success_rate > 50 THEN r.adjusted_pt_current END) AS avg_combined_criteria
        FROM (
            SELECT 
                ticker,
                analyst_name,
                MAX(date) AS latest_date
            FROM ratings
            GROUP BY ticker, analyst_name
        ) AS latest_ratings
        JOIN ratings AS r 
        ON latest_ratings.ticker = r.ticker 
        AND latest_ratings.analyst_name = r.analyst_name 
        AND latest_ratings.latest_date = r.date
        JOIN analysts AS a 
        ON r.analyst_name = a.name_full
        GROUP BY r.ticker
    """
    cursor.execute(query)
    return cursor.fetchall()
