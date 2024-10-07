import requests
import sqlite3
import datetime
import time
import os
import logging
import http.client as http_client

# Enable HTTP request logging to debug what's being sent
http_client.HTTPConnection.debuglevel = 1
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# Set up your API key and endpoint
HELIUS_API_URL = "https://api.helius.xyz/v0/addresses/{address}/transactions"
HELIUS_API_KEY = ""  # Your correct API key

# Serum market address for TBALL/USDC pair
TBALL_MARKET_ADDRESS = "FY5YpKXdBnAvQfzb9cfzyZ2DgB4yGshCpiRrHSYyJQig"  # This is the market address you found

# Connect to the existing transactions.db SQLite database
conn = sqlite3.connect('../../data/market_data/transactions.db')
cursor = conn.cursor()

# Create the order_book table in transactions.db if it doesn't already exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS order_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    bid_price REAL,
    bid_size REAL,
    ask_price REAL,
    ask_size REAL
)
''')
conn.commit()

# Function to fetch the order book data from Helius API
def fetch_order_book(address):
    try:
        headers = {
            'x-api-key': HELIUS_API_KEY  # Using x-api-key as required by Helius API
        }

        # Construct the full URL
        url = HELIUS_API_URL.format(address=address)

        # Print the URL and headers to verify correctness
        print(f"Requesting URL: {url}")
        print(f"Using API Key: {HELIUS_API_KEY}")

        # Make the request
        response = requests.get(url, headers=headers)

        # Log the status code and response for debugging
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")

        # Check if the request was successful
        if response.status_code == 200:
            transactions = response.json()

            # Assuming we're fetching recent transactions, extract bids/asks from transaction events
            for tx in transactions:
                bid_price = tx.get('bid_price', 0)
                bid_size = tx.get('bid_size', 0)
                ask_price = tx.get('ask_price', 0)
                ask_size = tx.get('ask_size', 0)

                # Insert data into the order_book table in transactions.db
                cursor.execute('''
                INSERT INTO order_book (timestamp, bid_price, bid_size, ask_price, ask_size)
                VALUES (?, ?, ?, ?, ?)
                ''', (datetime.datetime.now(), bid_price, bid_size, ask_price, ask_size))

            # Commit the transaction to the database
            conn.commit()
        else:
            print(f"Failed to fetch order book data: {response.status_code}, {response.text}")

    except Exception as e:
        print(f"Error fetching order book from Helius API: {e}")


# Main function to fetch order book data at regular intervals
def collect_order_book_data(address, interval=10):
    try:
        while True:
            fetch_order_book(address)
            # Wait for the defined interval (e.g., 10 seconds) before fetching the next snapshot
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Order book collection stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    # Call the data collection function for TBALL/USDC using the correct Serum market address
    collect_order_book_data(TBALL_MARKET_ADDRESS, interval=10)
