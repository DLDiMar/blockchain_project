import sys
import sqlite3
from web3 import Web3

# Block Crawler: retrieves transactions within a given block range and persists to a database.

def connect_to_database(db_path):
    # Connect to the SQLite database
    return sqlite3.connect(db_path)

def create_transaction_table(conn):
    # Create a table to store transactions if it doesn't exist
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            block_number INTEGER,
            transaction_index INTEGER,
            hash TEXT PRIMARY KEY,
            from_address TEXT,
            to_address TEXT,
            value TEXT,
            gas_price TEXT,
            gas_used INTEGER,
            timestamp INTEGER
        )
    ''')
    conn.commit()

def retrieve_transactions(endpoint, start_block, end_block):
    # Connect to Ethereum node
    web3 = Web3(Web3.HTTPProvider(endpoint))
    transactions = []

    # Loop through each block in the specified range
    for block_number in range(start_block, end_block + 1):
        # Retrieve block information
        block = web3.eth.get_block(block_number)
        if block is None or 'transactions' not in block:
            continue
        # Loop through transactions in the block
        for tx_hash in block['transactions']:
            tx = web3.eth.get_transaction(tx_hash)
            timestamp = web3.eth.get_block(block_number).timestamp
            transactions.append({
                'block_number': block_number,
                'transaction_index': tx['transactionIndex'],
                'hash': tx['hash'].hex(),
                'from_address': tx['from'],
                'to_address': tx['to'],
                'value': web3.fromWei(tx['value'], 'ether'),
                'gas_price': web3.fromWei(tx['gasPrice'], 'gwei'),
                'gas_used': tx['gas'],
                'timestamp': timestamp
            })

    return transactions

def persist_transactions(conn, transactions):
    # Insert transactions into the database
    cursor = conn.cursor()
    for tx in transactions:
        cursor.execute('''
            INSERT OR IGNORE INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (tx['block_number'], tx['transaction_index'], tx['hash'], tx['from_address'], tx['to_address'], tx['value'], tx['gas_price'], tx['gas_used'], tx['timestamp']))
    conn.commit()

def main():
    # Check if command line arguments are provided correctly
    if len(sys.argv) != 4:
        print("Usage: python block_crawler.py <endpoint> <db_path> <block_range>")
        sys.exit(1)

    # Parse command line arguments
    endpoint = sys.argv[1]
    db_path = sys.argv[2]
    block_range = sys.argv[3]

    # Extract start and end block numbers
    start_block, end_block = map(int, block_range.split('-'))

    # Connect to the database and create transaction table
    conn = connect_to_database(db_path)

    # Create transaction table if it doesn't exist
    create_transaction_table(conn)
    
    # Retrieve transactions within the specified range
    transactions = retrieve_transactions(endpoint, start_block, end_block)

    # Persist transactions to the database
    persist_transactions(conn, transactions)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
