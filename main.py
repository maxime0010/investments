import os
import requests
import benzinga
import sys

token = os.getenv("BENZINGA_API_KEY")
if not token:
    raise ValueError("No API key found in environment variables")

from benzinga import financial_data

bz = financial_data.Benzinga(token)
price = bz.delayed_quote(company_tickers = "AAPL") 


def main():
    try:
        print(bz.output(price))
        exit_program()
                
    except Exception as e:
        print(f"An error occurred: {e}")
        exit_program()

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

if __name__ == "__main__":
    main()
