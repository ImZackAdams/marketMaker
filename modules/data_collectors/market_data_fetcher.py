import requests
import json
import time
import os
import sqlite3
from datetime import datetime, timedelta

# Fetch the API key from environment variables
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

if not HELIUS_API_KEY:
    print("Error: Helius API key not found!")
    exit(1)

HELIUS_API_URL = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}"
TBALL_MINT_ADDRESS = "CWnzqQVFaD7sKsZyh116viC48G7qLz8pa5WhFpBEg9wM"
JUPITER_AGGREGATOR_MINTS = ["..."]  # Add known Jupiter aggregator mints
RAYDIUM_LIQUIDITY_MINTS = ["..."]   # Add known Raydium liquidity pool mints
RPC_URL = "https://api.mainnet-beta.solana.com"
MAX_RETRIES = 5  # Max number of retries when hitting rate limits
RETRY_DELAY = 20  # Delay in seconds after hitting a rate limit
BATCH_SIZE = 5  # Small batch size for processing transactions (for testing)
MAX_TRANSACTIONS = 10  # Fetch only 10 transactions for testing

# Construct the correct relative path to the database file
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/market_data/transactions.db'))

# Set up SQLite connection and schema
conn = sqlite3.connect(db_path)  # Use the correct relative path
cursor = conn.cursor()

# Create a table for transactions if not exists with UNIQUE constraint on transaction_signature
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_wallet TEXT,
        to_wallet TEXT,
        tball_sent REAL NULL,
        tball_received REAL NULL,
        token_transfered TEXT NULL,
        amount_transfered REAL NULL,
        transaction_signature TEXT UNIQUE,
        transaction_type TEXT,
        aggregator TEXT NULL,  
        timestamp INTEGER
    )
''')
conn.commit()

# Function to get TBALL transaction signatures (limited for testing)
def get_tball_signatures(mint_address, limit=MAX_TRANSACTIONS):
    signatures = []
    before_signature = None
    retries = 0

    while len(signatures) < limit and retries < MAX_RETRIES:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                mint_address,
                {
                    "limit": min(limit, limit - len(signatures)),
                    "before": before_signature
                }
            ]
        }

        response = requests.post(RPC_URL, json=payload)

        if response.status_code == 200:
            result = response.json().get('result', [])
            if not result:
                break  # No more signatures available

            signatures.extend([tx['signature'] for tx in result])
            before_signature = result[-1]['signature']
        elif response.status_code == 429:
            retries += 1
            print(f"Rate limit hit, retrying {retries}/{MAX_RETRIES} after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        else:
            print(f"Error fetching signatures: {response.status_code}")
            break

        time.sleep(0.2)

    return signatures

# Function to parse the transactions in small batches using the Helius API
def parse_transactions_in_batches(signatures, batch_size=MAX_TRANSACTIONS):
    headers = {
        "Content-Type": "application/json"
    }
    parsed_transactions = []

    # Only process one batch to limit API calls
    batch = signatures[:batch_size]
    body = {
        "transactions": batch
    }

    retries = 0
    while retries < MAX_RETRIES:
        response = requests.post(HELIUS_API_URL, headers=headers, data=json.dumps(body))

        if response.status_code == 200:
            parsed_transactions.extend(response.json())
            break
        elif response.status_code == 429:
            retries += 1
            print(f"Rate limit hit while parsing, retrying {retries}/{MAX_RETRIES} after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        else:
            print(f"Error parsing transactions: {response.status_code}")
            print(f"Response content: {response.content}")
            return None

    return parsed_transactions

# Function to check if a transaction involves Jupiter or Raydium aggregators
def identify_aggregator(transfer):
    if transfer.get("mint") in JUPITER_AGGREGATOR_MINTS:
        return "Jupiter"
    elif transfer.get("mint") in RAYDIUM_LIQUIDITY_MINTS:
        return "Raydium"
    return None

# Function to extract all transactions (including transfers and swaps with aggregators)
def extract_all_transactions(parsed_transactions):
    all_transactions = []

    for tx in parsed_transactions:
        if "tokenTransfers" not in tx:
            continue

        # Initialize variables to track swap information
        tball_transfers = []
        wsol_transfers = []
        other_transfers = []
        from_wallet = "Unknown"
        to_wallet = "Unknown"
        aggregators = set()

        # Loop through token transfers to identify swaps and fee transfers
        for transfer in tx.get("tokenTransfers", []):
            if transfer.get("mint") == TBALL_MINT_ADDRESS:
                tball_transfers.append(transfer)
                from_wallet = transfer.get("fromUserAccount", from_wallet)
                to_wallet = transfer.get("toUserAccount", to_wallet)
            elif transfer.get("mint") == "So11111111111111111111111111111111111111112":  # WSOL mint address
                wsol_transfers.append(transfer)
            else:
                other_transfers.append(transfer)

            # Check for known aggregators
            if "Jupiter" in transfer.get("toUserAccount", ""):
                aggregators.add("Jupiter")
            elif "Raydium" in transfer.get("toUserAccount", ""):
                aggregators.add("Raydium")

        # Determine transaction type
        if tball_transfers and wsol_transfers:
            transaction_type = "Swap"
        elif len(tball_transfers) > 1 or len(wsol_transfers) > 1:
            transaction_type = "MultiSwap"
        else:
            transaction_type = "Transfer"

        # Calculate total amounts
        tball_sent = sum(float(t.get("tokenAmount", 0)) for t in tball_transfers if t.get("fromUserAccount"))
        tball_received = sum(float(t.get("tokenAmount", 0)) for t in tball_transfers if t.get("toUserAccount"))
        wsol_sent = sum(float(t.get("tokenAmount", 0)) for t in wsol_transfers if t.get("fromUserAccount"))
        wsol_received = sum(float(t.get("tokenAmount", 0)) for t in wsol_transfers if t.get("toUserAccount"))

        transaction = {
            "from_wallet": from_wallet,
            "to_wallet": to_wallet,
            "tball_sent": tball_sent,
            "tball_received": tball_received,
            "wsol_sent": wsol_sent,
            "wsol_received": wsol_received,
            "transaction_signature": tx.get("signature"),
            "transaction_type": transaction_type,
            "aggregator": ", ".join(aggregators) if aggregators else None,
            "timestamp": tx.get("timestamp")
        }
        all_transactions.append(transaction)

    return all_transactions

# Function to save transactions into the SQLite database
def save_all_transactions_to_db(transactions):
    for tx in transactions:
        try:
            cursor.execute('''
                INSERT INTO transactions (from_wallet, to_wallet, tball_sent, tball_received, token_transfered, amount_transfered, transaction_signature, transaction_type, aggregator, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (tx['from_wallet'], tx['to_wallet'], tx['tball_sent'], tx['tball_received'],
                  None, None, tx['transaction_signature'], tx['transaction_type'], tx['aggregator'], tx['timestamp']))
        except sqlite3.IntegrityError:
            print(f"Transaction {tx['transaction_signature']} already exists in the database, skipping insert.")

    conn.commit()

if __name__ == "__main__":
    # Fetch TBALL transaction signatures (limited for testing)
    tball_signatures = get_tball_signatures(TBALL_MINT_ADDRESS, limit=MAX_TRANSACTIONS)

    if tball_signatures:
        parsed_transactions = parse_transactions_in_batches(tball_signatures, batch_size=MAX_TRANSACTIONS)

        if parsed_transactions:
            all_transactions = extract_all_transactions(parsed_transactions)
            if all_transactions:
                print(f"\n--- Summary ---\nTotal transactions found: {len(all_transactions)}\n")
                for idx, tx in enumerate(all_transactions, start=1):
                    readable_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(tx['timestamp']))
                    print(f"Transaction {idx}:")
                    print(f"  From Wallet: {tx['from_wallet']}")
                    print(f"  To Wallet: {tx['to_wallet']}")
                    print(f"  TBALL Sent: {tx.get('tball_sent', 'N/A')}")
                    print(f"  TBALL Received: {tx.get('tball_received', 'N/A')}")
                    print(f"  WSOL Sent: {tx.get('wsol_sent', 'N/A')}")
                    print(f"  WSOL Received: {tx.get('wsol_received', 'N/A')}")
                    print(f"  Transaction Type: {tx['transaction_type']}")
                    print(f"  Aggregator: {tx['aggregator']}")
                    print(f"  Transaction Signature: {tx['transaction_signature']}")
                    print(f"  Timestamp: {readable_date}")
                    print("-" * 40)

                # Save transactions to the SQLite database
                save_all_transactions_to_db(all_transactions)
                print("Transactions saved to SQLite database.")
            else:
                print("No transactions found.")
        else:
            print("Failed to parse TBALL transactions.")
    else:
        print("No transaction signatures fetched.")

    print("\nAPI calls made:")
    print("1 call to Solana RPC")
    print("1 call to Helius API")
    print("Total: 2 API calls")