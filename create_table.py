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
    input_amount INTEGER,
    output_amount INTEGER,
    relayer TEXT,
    is_success BOOLEAN,
    gas INTEGER,
    origin_chain TEXT,
    aim_chain TEXT,
    deposit_id TEXT,
    timestamp INTEGER,
    block INTEGER,
    FOREIGN KEY (deposit_id) REFERENCES Deposit(deposit_id)
)
''')

# Create Return table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Return (
    tx_hash TEXT,
    output_token TEXT,
    output_amount INTEGER,
    aim_chain TEXT,
    block INTEGER,
    PRIMARY KEY (tx_hash, output_token)
)
''')

# Create LastBlock table to record the last block number of each chain
cursor.execute('''
CREATE TABLE IF NOT EXISTS LastBlock (
    chain_name TEXT PRIMARY KEY,
    last_block INTEGER
)
''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()
