import sys
import sqlite3

def query_block_timeframe(db_name, start_time_epoch, end_time_epoch):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Check if the transactions table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
    table_exists = cursor.fetchone()
    if not table_exists:
        print("The 'transactions' table does not exist in the database.")
        conn.close()
        return

    # Query to retrieve the block number and total volume transferred for the block with the largest volume
    query = """
        SELECT block_number, SUM(value)
        FROM transactions
        WHERE block_number BETWEEN 18908800 AND 18909050
            AND timestamp BETWEEN ? AND ?
        GROUP BY block_number
        ORDER BY SUM(value) DESC
        LIMIT 1
    """

    # Execute the query with the epoch time range parameters
    cursor.execute(query, (start_time_epoch, end_time_epoch))

    # Fetch the result
    result = cursor.fetchone()

    # Print the result
    if result:
        block_number, total_volume = result
        print(f"Block Number: {block_number}, Total Volume Transferred: {total_volume}")
    else:
        print("No data found for the specified time range.")

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    # Check if command line arguments are provided correctly
    if len(sys.argv) != 4:
        print("Usage: python3 query_block_timeframe.py <db_name> <start_time_epoch> <end_time_epoch>")
        sys.exit(1)

    # Parse command line arguments
    db_name = sys.argv[1]
    start_time_epoch = int(sys.argv[2])
    end_time_epoch = int(sys.argv[3])

    # Call the function with provided arguments
    query_block_timeframe(db_name, start_time_epoch, end_time_epoch)
