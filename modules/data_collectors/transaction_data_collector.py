import requests
import json
import time
import os
import sqlite3
from datetime import datetime

# Fetch the API key from environment variables
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

if not HELIUS_API_KEY:
    print("Error: Helius API key not found!")
    exit(1)

TBALL_MINT_ADDRESS = "CWnzqQVFaD7sKsZyh116viC48G7qLz8pa5WhFpBEg9wM"
WSOL_MINT_ADDRESS = "So11111111111111111111111111111111111111112"
MAX_RETRIES = 5
RETRY_DELAY = 20
FETCH_LIMIT = 20  # Fetch only the most recent 20 transactions as a test

# Construct the correct relative path to the database file
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/market_data/transactions.db'))

# Set up SQLite connection and schema
def setup_database():
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,  -- Remove AUTOINCREMENT to prevent unwanted increments
                signature TEXT UNIQUE,
                timestamp INTEGER,
                transaction_type TEXT,
                from_wallet TEXT,
                to_wallet TEXT,
                tball_amount REAL,
                wsol_amount REAL,
                other_token_amount REAL,
                other_token_mint TEXT,
                aggregators TEXT,  
                referral_fees REAL,  
                referral_vault TEXT,  
                intermediate_swaps TEXT,  
                fee REAL,
                success BOOLEAN,
                raw_data TEXT  
            )
        ''')
        conn.commit()

# Fetch the most recent TBALL transactions
def get_tball_transactions(limit=FETCH_LIMIT):
    """Fetch the most recent TBALL transactions."""
    url = f"https://api.helius.xyz/v0/addresses/{TBALL_MINT_ADDRESS}/transactions?api-key={HELIUS_API_KEY}&limit={limit}"

    retries = 0
    while retries < MAX_RETRIES:
        print(f"Fetching the most recent {limit} TBALL transactions...")
        response = requests.get(url)
        if response.status_code == 200:
            transactions = response.json()
            print(f"Fetched {len(transactions)} transactions.")
            return transactions[:limit]
        elif response.status_code == 429:
            retries += 1
            print(f"Rate limit hit, retrying {retries}/{MAX_RETRIES} after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
        else:
            print(f"Error fetching TBALL transactions: {response.status_code}")
            print(f"Response content: {response.content}")
            break
    return []

# Extract relevant transaction data
def extract_transaction_data(tx):
    """Extracts relevant transaction data from the raw JSON."""
    tball_transfers = []
    wsol_transfers = []
    other_transfers = []
    aggregators = set()
    referral_fees = 0
    referral_vault = None
    intermediate_swaps = []

    # Extract fee
    fee = tx.get('fee', 0) / 1e9  # Convert lamports to SOL

    # Detect aggregators
    if 'Jupiter' in str(tx):
        aggregators.add('Jupiter')
    if 'Raydium' in str(tx):
        aggregators.add('Raydium')
    if 'Lifinity' in str(tx):
        aggregators.add('Lifinity')

    # Process token transfers
    for transfer in tx.get('tokenTransfers', []):
        mint = transfer.get('mint')
        if mint == TBALL_MINT_ADDRESS:
            tball_transfers.append(transfer)
        elif mint == WSOL_MINT_ADDRESS:
            wsol_transfers.append(transfer)
        else:
            other_transfers.append(transfer)

    # Capture referral fees
    for transfer in other_transfers:
        if 'Jupiter Partner Referral Fee Vault' in str(transfer['toUserAccount']):
            referral_fees += float(transfer['tokenAmount'])
            referral_vault = transfer['toUserAccount']

    # Capture intermediate swaps
    swap_events = tx.get('events', {}).get('swap', {}).get('innerSwaps', [])
    for event in swap_events:
        for token_input in event.get('tokenInputs', []):
            intermediate_swaps.append(f"Input: {token_input['tokenAmount']} {token_input['mint']}")
        for token_output in event.get('tokenOutputs', []):
            intermediate_swaps.append(f"Output: {token_output['tokenAmount']} {token_output['mint']}")

    # Calculate amounts
    tball_amount = sum(float(t['tokenAmount']) for t in tball_transfers)
    wsol_amount = sum(float(t['tokenAmount']) for t in wsol_transfers)
    other_amount = sum(float(t['tokenAmount']) for t in other_transfers)
    other_mint = other_transfers[0]['mint'] if other_transfers else None

    # Determine transaction type
    if tball_transfers and (wsol_transfers or other_transfers):
        transaction_type = 'Swap'
    elif len(tball_transfers) > 1 or len(wsol_transfers) > 1:
        transaction_type = 'MultiSwap'
    else:
        transaction_type = 'Transfer'

    # Get 'from' and 'to' wallets
    from_wallet = tx.get('feePayer') or (tball_transfers[0]['fromUserAccount'] if tball_transfers else None)
    to_wallet = tball_transfers[-1]['toUserAccount'] if tball_transfers else None

    return {
        'signature': tx.get('signature', ''),
        'timestamp': tx.get('timestamp', 0) // 1000,  # Convert milliseconds to seconds
        'transaction_type': transaction_type,
        'from_wallet': from_wallet,
        'to_wallet': to_wallet,
        'tball_amount': tball_amount,
        'wsol_amount': wsol_amount,
        'other_token_amount': other_amount,
        'other_token_mint': other_mint,
        'aggregators': ', '.join(aggregators) if aggregators else None,
        'referral_fees': referral_fees,
        'referral_vault': referral_vault,
        'intermediate_swaps': ', '.join(intermediate_swaps),
        'fee': fee,
        'success': tx.get('err') is None,
        'raw_data': json.dumps(tx)
    }

# Safe extraction of transaction data with error handling
def safe_extract_transaction_data(tx):
    """Safely extract transaction data and handle any errors."""
    try:
        return extract_transaction_data(tx)
    except Exception as e:
        print(f"Error processing transaction {tx.get('signature', 'Unknown')}: {str(e)}")
        return None

# Save transactions to database using context manager
def save_transactions_to_db(transactions):
    """Save extracted transactions to the SQLite database."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for tx in transactions:
            # Check if the transaction already exists based on the signature
            cursor.execute("SELECT id FROM transactions WHERE signature = ?", (tx['signature'],))
            result = cursor.fetchone()

            if result:
                # If the transaction already exists, update the existing row
                cursor.execute('''
                    UPDATE transactions
                    SET timestamp = ?, transaction_type = ?, from_wallet = ?, to_wallet = ?,
                        tball_amount = ?, wsol_amount = ?, other_token_amount = ?, other_token_mint = ?,
                        aggregators = ?, referral_fees = ?, referral_vault = ?, intermediate_swaps = ?,
                        fee = ?, success = ?, raw_data = ?
                    WHERE signature = ?
                ''', (
                    tx['timestamp'], tx['transaction_type'], tx['from_wallet'], tx['to_wallet'],
                    tx['tball_amount'], tx['wsol_amount'], tx['other_token_amount'], tx['other_token_mint'],
                    tx['aggregators'], tx['referral_fees'], tx['referral_vault'], tx['intermediate_swaps'],
                    tx['fee'], tx['success'], tx['raw_data'], tx['signature']
                ))
                print(f"Transaction {tx['signature']} updated in the database.")
            else:
                # If the transaction doesn't exist, insert a new row
                cursor.execute('''
                    INSERT INTO transactions
                    (signature, timestamp, transaction_type, from_wallet, to_wallet,
                     tball_amount, wsol_amount, other_token_amount, other_token_mint,
                     aggregators, referral_fees, referral_vault, intermediate_swaps,
                     fee, success, raw_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tx['signature'], tx['timestamp'], tx['transaction_type'],
                    tx['from_wallet'], tx['to_wallet'], tx['tball_amount'],
                    tx['wsol_amount'], tx['other_token_amount'], tx['other_token_mint'],
                    tx['aggregators'], tx['referral_fees'], tx['referral_vault'], tx['intermediate_swaps'],
                    tx['fee'], tx['success'], tx['raw_data']
                ))
                print(f"Transaction {tx['signature']} inserted into the database.")

        conn.commit()

# Main function to fetch, process, and save transactions
def main():
    """Main function to fetch, process, and save the most recent transactions."""
    print(f"Starting to fetch the most recent {FETCH_LIMIT} transactions...")

    # Fetch the most recent TBALL transactions
    tball_transactions = get_tball_transactions(limit=FETCH_LIMIT)

    if tball_transactions:
        print(f"Processing {len(tball_transactions)} transactions...")

        # Extract transaction data and process it
        all_transactions = [safe_extract_transaction_data(tx) for tx in tball_transactions if tx]
        all_transactions = [tx for tx in all_transactions if tx is not None]

        if all_transactions:
            # Save the transactions to the database
            save_transactions_to_db(all_transactions)
            print(f"Successfully saved {len(all_transactions)} transactions.")
        else:
            print("No valid transactions to process.")
    else:
        print("No transactions were fetched.")


if __name__ == "__main__":
    setup_database()  # Ensure database and tables are set up
    main()
