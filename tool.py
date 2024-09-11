import os
from web3 import Web3
import json
import sqlite3


def get_variable(name):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM Variable WHERE name = ?", (name,))
    result = cursor.fetchone()
    if not result:
        return 1
    return result[0]

# insert the value into the Variable table or update the value
def update_variable(name, value):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM Variable WHERE name = ?", (name,))
    result = cursor.fetchone()
    if result:
        cursor.execute("UPDATE Variable SET value = ? WHERE name = ?", (value, name))
    else:
        cursor.execute("INSERT INTO Variable (name, value) VALUES (?, ?)", (name, value))
    conn.commit()

def update_the_bundle(chain, start_block, start_id):
    abi = ""
    with open("hub_abi.json", "r", encoding="utf-8") as file:
        abi = json.load(file)
    eth_rpc = os.getenv("ETH_RPC")
    web3 = Web3(Web3.HTTPProvider(eth_rpc))
    contract_address = web3.to_checksum_address(
        "0xc186fA914353c44b2E33eBE05f21846F1048bEda"
    )
    contract = web3.eth.contract(address=contract_address, abi=abi)
    bundle_id = int(get_variable(f"last_{chain}_bundle_id"))
    if bundle_id > start_id:
        start_id = bundle_id
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    event_list = contract.events.ProposeRootBundle.create_filter(
        from_block=14819537
    ).get_all_entries()
    for event in event_list:
        bundle_numbers = event['args'].get('bundleEvaluationBlockNumbers', [])
        if chain == 'base' and len(bundle_numbers) >= 7:
            if bundle_numbers[6] >= start_block:
                cursor.execute(
                    """
                    INSERT INTO Bundle (
                    chain, bundle_id, end_block, refund_root
                    ) VALUES (?, ?, ?, ?) 
                    """,
                    (
                        chain,
                        start_id,
                        bundle_numbers[6],
                        event['args']['relayerRefundRoot']
                    )
                )
                start_id += 1
    conn.commit()
    update_variable(f"last_{chain}_bundle_id", start_id)

def get_bundle_id(block, cursor):
    cursor.execute("SELECT bundle_id FROM Bundle WHERE end_block >= ?", (block,))
    result = cursor.fetchall()
    if not result:
        return 0
    return result[0][0]

def get_relayer_root(chain, cursor, bundle):
    cursor.execute("SELECT refund_root FROM Bundle WHERE chain = ? AND bundle_id = ?", (chain, bundle))
    result = cursor.fetchall()
    if not result:
        return ''
    hex_str = result[0][0].hex()
    return hex_str