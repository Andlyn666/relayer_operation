import requests
import sqlite3
from web3 import Web3
import json
import os
from tool import create_w3_contract

def decode_input_data(input_data, contract):
    decoded_data = contract.decode_function_input(input_data)
    return decoded_data


def insert_return_data(contract, cursor, web3, block=17987144):
    event_list = contract.events.ExecutedRelayerRefundRoot.create_filter(
        from_block=block
    ).get_all_entries()
    for event in event_list:
        refund_addresses = event["args"]["refundAddresses"]
        address_to_check = "0x84A36d2C3d2078c560Ff7b62815138a16671b549"
        if address_to_check in refund_addresses:
            block = web3.eth.get_block(event["blockNumber"])
            # Get the timestamp from the block details
            timestamp = block["timestamp"]
            indices = [
                i for i, addr in enumerate(refund_addresses) if addr == address_to_check
            ]
            for index in indices:
                try:
                    cursor.execute(
                        """
                        INSERT INTO Return (
                        tx_hash, output_token, output_amount, aim_chain, block, time_stamp, bundle_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                  """,
                        (
                            event["transactionHash"].hex(),
                            event["args"]["l2TokenAddress"],
                            str(event["args"]["refundAmounts"][index]),
                            "base",
                            event["blockNumber"],
                            timestamp,
                            event["args"]["rootBundleId"],
                        ),
                    )
                except sqlite3.IntegrityError:
                    continue
    return


def get_latest_base_block(cursor):
    # Get the last block number from Variable table
    cursor.execute("SELECT value FROM Variable WHERE name = ?", ("base_block",))
    result = cursor.fetchone()
    last_block = int(result[0]) + 1 if result else 17987144
    return last_block


def update_base():
    print("Updating base")
    global op_spoke, base_spoke, arb_spoke, eth_spoke
    op_spoke, base_spoke, arb_spoke, eth_spoke = create_w3_contract()
    base_rpc = os.getenv("BASE_RPC")
    web3 = Web3(Web3.HTTPProvider(base_rpc))
    contract = base_spoke
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()

    last_block = get_latest_base_block(cursor)
    
    base_key = os.getenv("BASE_KEY")
    # Define the URL with query parameters
    url = f"https://api.basescan.org/api?module=account&action=txlist&address=0x84A36d2C3d2078c560Ff7b62815138a16671b549&startblock={last_block}&endblock=99999999&sort=asc&apikey={base_key}"
    response = requests.get(url)
    data = response.json()
    for tx in data["result"]:
        if tx["methodId"] == "0x2e378115":
            decode_input = decode_input_data(tx["input"], contract)
            decode_input = decode_input[1]["relayData"]
            gas_used = str(
                int(tx["gasUsed"]) * int(tx["gasPrice"])
            )  # Correctly convert gas value to string
            cursor.execute(
                """
               INSERT INTO Fill (
                  tx_hash, relayer, is_success, gas,
                  aim_chain, time_stamp, block, origin_chain,
                        input_amount, output_amount, deposit_id,
                        input_token, output_token
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         """,
                (
                    tx["hash"],
                    "0x84A36d2C3d2078c560Ff7b62815138a16671b549",
                    tx["txreceipt_status"] == "1",
                    gas_used,
                    "base",
                    int(tx["timeStamp"]),
                    int(tx["blockNumber"]),
                    decode_input["originChainId"],
                    decode_input["inputAmount"],
                    decode_input["outputAmount"],
                    decode_input["depositId"],
                    decode_input["inputToken"],
                    decode_input["outputToken"]
                ),
            )

        # Insert last block number to LastBlock table, update if chain_name already exists
        cursor.execute(
            """
         INSERT INTO Variable (
               name, value
         ) VALUES (?, ?)
         ON CONFLICT(name) DO UPDATE SET value=excluded.value
        """,
            ("base_block", int(tx["blockNumber"])),
        )
    insert_return_data(contract, cursor, web3, last_block)
    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
