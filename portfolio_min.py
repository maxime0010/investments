def fetch_portfolio_for_date(cursor, date):
    query = """
        SELECT ticker, expected_return_combined_criteria, last_closing_price
        FROM analysis_simulation
        WHERE date = %s 
        AND num_combined_criteria >= %s
        AND stddev_combined_criteria <= 100  -- Criterion: standard deviation <= 100
        AND expected_return_combined_criteria >= 25  -- Minimum expected return of 25%
        ORDER BY expected_return_combined_criteria DESC
    """
    print(f"[DEBUG] Fetching portfolio for {date} with expected return >= 25%")
    cursor.execute(query, (date, MIN_ANALYSTS))
    result = cursor.fetchall()
    print(f"[DEBUG] Retrieved portfolio: {result}")
    return result

def simulate_portfolio(retries=3):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Initialize the first portfolio value (100 total to be reinvested)
        initial_date = date_list[0]
        initial_portfolio = fetch_portfolio_for_date(cursor, initial_date)
        closing_prices = get_closing_prices_as_of(cursor, initial_date)
        
        total_portfolio_value = Decimal(100)
        portfolio_value = []

        if initial_portfolio:
            equal_value_per_stock = total_portfolio_value / Decimal(len(initial_portfolio))  # Adjust based on the number of stocks
            print(f"[DEBUG] Initial portfolio: {initial_portfolio}")
            for row in initial_portfolio:
                ticker, expected_return, last_closing_price = row[:3]
                print(f"[DEBUG] Processing stock: {ticker}, expected_return: {expected_return}, last_closing_price: {last_closing_price}")
                quantity = equal_value_per_stock / last_closing_price
                portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
        
            # Prepare data for the first batch insert
            portfolio_data = [(initial_date, ranking + 1, ticker, stock_price, quantity, total_value)
                              for ranking, (ticker, stock_price, quantity, total_value) in enumerate(portfolio_value)]
            batch_insert_portfolio_simulation(cursor, portfolio_data)
        
        # Process for each subsequent week
        for date in date_list[1:]:
            for attempt in range(retries):
                try:
                    # Fetch closing prices as of this date
                    closing_prices = get_closing_prices_as_of(cursor, date)
                    
                    # Calculate the portfolio value based on the previous week
                    total_portfolio_value, portfolio_value = calculate_portfolio_value(cursor, date, portfolio_value, closing_prices)
                    
                    # Fetch the new portfolio and rebalance
                    new_portfolio = fetch_portfolio_for_date(cursor, date)
                    
                    if new_portfolio:
                        equal_value_per_stock = total_portfolio_value / Decimal(len(new_portfolio))  # Adjust based on the number of stocks
                        new_portfolio_value = []
                        
                        print(f"[DEBUG] New portfolio for {date}: {new_portfolio}")
                        for row in new_portfolio:
                            ticker, expected_return, last_closing_price = row[:3]
                            print(f"[DEBUG] Processing stock: {ticker}, expected_return: {expected_return}, last_closing_price: {last_closing_price}")
                            quantity = equal_value_per_stock / last_closing_price
                            new_portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
                        
                        # Prepare batch insert data
                        portfolio_data = [(date, ranking + 1, ticker, stock_price, quantity, total_value)
                                          for ranking, (ticker, stock_price, quantity, total_value) in enumerate(new_portfolio_value)]
                        batch_insert_portfolio_simulation(cursor, portfolio_data)

                    # Update the previous portfolio with sell details
                    update_existing_portfolio(cursor, date, closing_prices)

                    conn.commit()
                    break  # Break the retry loop if successful

                except mysql.connector.Error as err:
                    if err.errno == 1213:  # Deadlock error
                        print(f"Deadlock detected on {date}. Retrying... attempt {attempt + 1}")
                        conn.rollback()  # Rollback the transaction
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        raise  # Re-raise other MySQL errors

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        if conn:
            conn.rollback()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        cursor.close()
        conn.close()

# Run the simulation
simulate_portfolio()
