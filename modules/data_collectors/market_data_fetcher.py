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
FETCH_LIMIT = 50  # Increased to fetch more transactions

# Construct the correct relative path to the database file
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/market_data/transactions.db'))

# Set up SQLite connection and schema
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create an enhanced table for transactions
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        fee REAL,
        success BOOLEAN,
        raw_data TEXT
    )
''')
conn.commit()

def get_tball_transactions(limit=FETCH_LIMIT):
    url = f"https://api.helius.xyz/v0/addresses/{TBALL_MINT_ADDRESS}/transactions?api-key={HELIUS_API_KEY}&limit={limit}"
    retries = 0

    while retries < MAX_RETRIES:
        response = requests.get(url)
        if response.status_code == 200:
            transactions = response.json()
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

def extract_transaction_data(tx):
    tball_transfers = []
    wsol_transfers = []
    other_transfers = []
    aggregators = set()

    # Extract fee
    fee = tx.get('fee', 0) / 1e9  # Convert lamports to SOL

    # Detect aggregators
    if 'Jupiter' in str(tx):
        aggregators.add('Jupiter')
    if 'Raydium' in str(tx):
        aggregators.add('Raydium')

    # Process token transfers
    for transfer in tx.get('tokenTransfers', []):
        mint = transfer.get('mint')
        if mint == TBALL_MINT_ADDRESS:
            tball_transfers.append(transfer)
        elif mint == WSOL_MINT_ADDRESS:
            wsol_transfers.append(transfer)
        else:
            other_transfers.append(transfer)

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
        'fee': fee,
        'success': tx.get('err') is None,
        'raw_data': json.dumps(tx)
    }

def safe_extract_transaction_data(tx):
    try:
        return extract_transaction_data(tx)
    except Exception as e:
        print(f"Error processing transaction {tx.get('signature', 'Unknown')}: {str(e)}")
        return None

def save_transactions_to_db(transactions):
    for tx in transactions:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO transactions
                (signature, timestamp, transaction_type, from_wallet, to_wallet,
                 tball_amount, wsol_amount, other_token_amount, other_token_mint,
                 aggregators, fee, success, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx['signature'], tx['timestamp'], tx['transaction_type'],
                tx['from_wallet'], tx['to_wallet'], tx['tball_amount'],
                tx['wsol_amount'], tx['other_token_amount'], tx['other_token_mint'],
                tx['aggregators'], tx['fee'], tx['success'], tx['raw_data']
            ))
        except sqlite3.IntegrityError:
            print(f"Transaction {tx['signature']} already exists in the database, updating.")

    conn.commit()

def fetch_specific_transaction(signature):
    url = f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
    payload = {"transactions": [signature]}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        transactions = response.json()
        if transactions:
            return transactions[0]
    return None

def main():
    tball_transactions = get_tball_transactions(limit=FETCH_LIMIT)

    # Fetch the specific transaction
    specific_tx = fetch_specific_transaction('2ufHFwWXHSdhLJPEKFHehmTojjBoeoMvotd13xYHb5Z5GgdNSDYSPcRQ5yx7fuR7fTKnnWxx2wDVkaP55KmuikQd')
    if specific_tx:
        tball_transactions.insert(0, specific_tx)

    if tball_transactions:
        all_transactions = [safe_extract_transaction_data(tx) for tx in tball_transactions if tx]
        all_transactions = [tx for tx in all_transactions if tx is not None]

        print(f"\n--- Summary ---\nTotal transactions found: {len(all_transactions)}\n")
        for idx, tx in enumerate(all_transactions, start=1):
            readable_date = datetime.fromtimestamp(tx['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Transaction {idx}:")
            print(f"  Signature: {tx['signature']}")
            print(f"  Type: {tx['transaction_type']}")
            print(f"  From: {tx['from_wallet']}")
            print(f"  To: {tx['to_wallet']}")
            print(f"  TBALL Amount: {tx['tball_amount']}")
            print(f"  WSOL Amount: {tx['wsol_amount']}")
            print(f"  Other Token Amount: {tx['other_token_amount']}")
            print(f"  Other Token Mint: {tx['other_token_mint']}")
            print(f"  Aggregators: {tx['aggregators']}")
            print(f"  Fee: {tx['fee']}")
            print(f"  Success: {tx['success']}")
            print(f"  Timestamp: {readable_date}")
            print("-" * 40)

        save_transactions_to_db(all_transactions)
        print("Transactions saved to SQLite database.")
    else:
        print("No TBALL transactions fetched.")

if __name__ == "__main__":
    main()
    conn.close()