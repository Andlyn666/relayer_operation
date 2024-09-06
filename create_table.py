import sqlite3

# Connect to SQLite database (it will create the database file if it doesn't exist)
conn = sqlite3.connect('mydatabase.db')

# Create a cursor object
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS Fill')
cursor.execute('DROP TABLE IF EXISTS Return')
cursor.execute('DROP TABLE IF EXISTS LastBlock')

# Create Fill table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Fill (
    tx_hash TEXT PRIMARY KEY,
    input_token TEXT,
    output_token TEXT,
    input_amount TEXT,
    output_amount TEXT,
    relayer TEXT,
    is_success BOOLEAN,
    gas TEXT,
    origin_chain TEXT,
    aim_chain TEXT,
    deposit_id TEXT,
    time_stamp TEXT,
    block TEXT,
    FOREIGN KEY (deposit_id) REFERENCES Deposit(deposit_id)
)
''')

# Create Return table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Return (
    tx_hash TEXT,
    output_token TEXT,
    output_amount TEXT,
    aim_chain TEXT,
    block TEXT,
    time_stamp TEXT,
    PRIMARY KEY (tx_hash, output_token, aim_chain)
)
''')

# Create LastBlock table to record the last block number of each chain
cursor.execute('''
CREATE TABLE IF NOT EXISTS Variable (
    name TEXT PRIMARY KEY,
    value TEXT
)
''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()
