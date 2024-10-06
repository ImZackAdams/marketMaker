import requests
import json

# Solana Mainnet Beta RPC URL
RPC_URL = "https://api.mainnet-beta.solana.com"

# Transaction signature you want to test
SIGNATURE = '2ufHFwWXHSdhLJPEKFHehmTojjBoeoMvotd13xYHb5Z5GgdNSDYSPcRQ5yx7fuR7fTKnnWxx2wDVkaP55KmuikQd'

def fetch_transaction_by_signature(signature):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "json",
                "maxSupportedTransactionVersion": 0  # Support versioned transactions up to version 0
            }
        ]
    }

    try:
        response = requests.post(RPC_URL, json=payload)
        if response.status_code == 200:
            transaction_data = response.json()
            print(json.dumps(transaction_data, indent=4))
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"An error occurred while fetching the transaction: {str(e)}")

if __name__ == "__main__":
    print(f"Fetching transaction with signature: {SIGNATURE}")
    fetch_transaction_by_signature(SIGNATURE)
