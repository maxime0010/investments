def simulate_portfolio(retries=3):
    """Main function to simulate the portfolio process."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Check if the table is empty
        latest_record = get_existing_latest_record(cursor)

        if not latest_record:
            # If table is empty, simulate from START_DATE
            print("[DEBUG] The table is empty. Starting from the beginning.")
            date_list = generate_date_list(START_DATE, END_DATE)
        else:
            # If latest_record is already a date, no conversion is needed
            if isinstance(latest_record, datetime):
                latest_record = latest_record.date()  # Convert datetime to date if needed

            print(f"[DEBUG] The table has records. Starting from {latest_record + timedelta(weeks=1)}.")
            start_date = latest_record + timedelta(weeks=1)
            date_list = generate_date_list(start_date, END_DATE)
        
        if not date_list:
            print("[DEBUG] No new dates to process.")
            return

        # Initialize the first portfolio value (100 total, 10 per stock)
        initial_date = date_list[0]
        initial_portfolio = fetch_portfolio_for_date(cursor, initial_date)
        closing_prices = get_closing_prices_as_of(cursor, initial_date)
        
        total_portfolio_value = Decimal(100)
        equal_value_per_stock = total_portfolio_value / Decimal(10)
        portfolio_value = []

        # Allocate initial value to each stock
        for row in initial_portfolio:
            ticker, expected_return, last_closing_price = row[:3]
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
                        equal_value_per_stock = total_portfolio_value / Decimal(10)
                        new_portfolio_value = []
                        
                        for row in new_portfolio:
                            ticker, expected_return, last_closing_price = row[:3]
                            quantity = equal_value_per_stock / last_closing_price
                            new_portfolio_value.append((ticker, last_closing_price, quantity, equal_value_per_stock))
                        
                        # Prepare batch insert data
                        portfolio_data = [(date, ranking + 1, ticker, stock_price, quantity, total_value)
                                          for ranking, (ticker, stock_price, quantity, total_value) in enumerate(new_portfolio_value)]
                        batch_insert_portfolio_simulation(cursor, portfolio_data)

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
