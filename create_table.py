import sqlite3

# Connect to SQLite database (it will create the database file if it doesn't exist)
conn = sqlite3.connect('mydatabase.db')

# Create a cursor object
cursor = conn.cursor()
# cursor.execute('DROP TABLE IF EXISTS Fill')
# cursor.execute('DROP TABLE IF EXISTS Return')
# cursor.execute('DROP TABLE IF EXISTS Variable')
cursor.execute('DROP TABLE IF EXISTS Bundle')

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
    deposit_time TEXT,
    time_stamp TEXT,
    block INTEGER,
    bundle_id TEXT,
    is_return BOOLEAN,
    lp_fee TEXT,
    repayment_chain TEXT
)
''')

# Create Return table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Return (
    tx_hash TEXT,
    output_token TEXT,
    output_amount TEXT,
    aim_chain TEXT,
    block INTEGER,
    time_stamp TEXT,
    bundle_id TEXT,
    PRIMARY KEY (tx_hash, output_token, aim_chain)
)
''')

# Create Variable table to record the last block number of each chain
cursor.execute('''
CREATE TABLE IF NOT EXISTS Variable (
    name TEXT PRIMARY KEY,
    value TEXT
)
''')

# Create Bundle table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Bundle (
    bundle_id TEXT,
    refund_root TEXT,
    chain TEXT,
    base_end_block INTEGER,
    op_end_block INTEGER,
    arb_end_block INTEGER,
    eth_end_block INTEGER,
    PRIMARY KEY (bundle_id, chain)
)
''')

# Create CEX_FEE table
cursor.execute('''
CREATE TABLE IF NOT EXISTS CEX_FEE (
    token TEXT,
    chain TEXT,
    fee TEXT,
    time_stamp TEXT,
    PRIMARY KEY (token, chain, time_stamp)
)
''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()
