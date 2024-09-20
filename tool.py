import os
from web3 import Web3
import json
import sqlite3
import requests

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
        cursor.execute(
            "INSERT INTO Variable (name, value) VALUES (?, ?)", (name, value)
        )
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
    propose_list = contract.events.ProposeRootBundle.create_filter(
        from_block=14819537
    ).get_all_entries()
    relayer_root_list = []
    for event in propose_list:
        relayer_root_list.append(event["args"]["relayerRefundRoot"])
    bundle_event_list = get_event_bundle_id(chain, relayer_root_list)
    for event in propose_list:
        for bundle_event in bundle_event_list:
            if event["args"]["relayerRefundRoot"] == bundle_event["args"]["relayerRefundRoot"]:
                bundle_id = bundle_event["args"]["rootBundleId"]
                bundle_numbers = event["args"].get("bundleEvaluationBlockNumbers", [])
                if chain == "base" and len(bundle_numbers) >= 7:
                    if bundle_numbers[6] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, end_block, refund_root
                            ) VALUES (?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                bundle_numbers[6],
                                event["args"]["relayerRefundRoot"],
                            ),
                        )
                if chain == "op" and len(bundle_numbers) >= 2:
                    if bundle_numbers[1] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, end_block, refund_root
                            ) VALUES (?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                bundle_numbers[1],
                                event["args"]["relayerRefundRoot"],
                            ),
                        )
                if chain == "arb" and len(bundle_numbers) >= 5:
                    if bundle_numbers[4] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, end_block, refund_root
                            ) VALUES (?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                bundle_numbers[4],
                                event["args"]["relayerRefundRoot"],
                            ),
                        )
                if chain == "eth" and len(bundle_numbers) >= 1:
                    if bundle_numbers[0] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, end_block, refund_root
                            ) VALUES (?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                bundle_numbers[0],
                                event["args"]["relayerRefundRoot"],
                            ),
                        )
    conn.commit()
    update_variable(f"last_{chain}_bundle_id", bundle_id)


def get_bundle_id(block, cursor, chain):
    cursor.execute(
        "SELECT bundle_id FROM Bundle WHERE end_block > ? AND chain = ?",
        (block, chain),
    )
    result = cursor.fetchall()
    if not result:
        return 0
    return result[0][0]


def get_relayer_root(chain, cursor, bundle):
    cursor.execute(
        "SELECT refund_root FROM Bundle WHERE chain = ? AND bundle_id = ?",
        (chain, bundle),
    )
    result = cursor.fetchall()
    if not result:
        return ""
    hex_str = result[0][0].hex()
    return hex_str

def create_w3_contract():
    global op_spoke, base_spoke, arb_spoke, eth_spoke
    abi = ""
    with open("spoke_abi.json", "r", encoding="utf-8") as file:
        abi = json.load(file)

    rpc = os.getenv("OP_RPC")
    w3_op = Web3(Web3.HTTPProvider(rpc))
    contract_address = w3_op.to_checksum_address(
        "0x6f26Bf09B1C792e3228e5467807a900A503c0281"
    )
    op_spoke = w3_op.eth.contract(address=contract_address, abi=abi)
    
    rpc = os.getenv("BASE_RPC")
    w3_base = Web3(Web3.HTTPProvider(rpc))
    contract_address = w3_base.to_checksum_address(
        "0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64"
    )
    base_spoke = w3_base.eth.contract(address=contract_address, abi=abi)

    rpc = os.getenv("ARB_RPC")
    w3_arb = Web3(Web3.HTTPProvider(rpc))
    contract_address = w3_arb.to_checksum_address(
        "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a"
    )
    arb_spoke = w3_arb.eth.contract(address=contract_address, abi=abi)

    rpc = os.getenv("ETH_RPC")
    w3_eth = Web3(Web3.HTTPProvider(rpc))
    contract_address = w3_arb.to_checksum_address(
        "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5"
    )
    eth_spoke = w3_eth.eth.contract(address=contract_address, abi=abi)
    return op_spoke, base_spoke, arb_spoke, eth_spoke

# listen to the V3FundsDeposited event and filter the deposit_id to get the quoteTimestamp
def get_deposit_time(deposit_id_array):
    contract = None
    start_block = 0
    contract = eth_spoke
    start_block = 20298487
    event_list = contract.events.V3FundsDeposited.create_filter(from_block = start_block, argument_filters={'depositId' : deposit_id_array}
    ).get_all_entries()

    contract = op_spoke
    start_block = 123750953
    event_list = event_list + contract.events.V3FundsDeposited.create_filter(from_block = start_block, argument_filters={'depositId' : deposit_id_array}).get_all_entries()

    contract = base_spoke
    start_block = 19043282
    event_list = event_list + contract.events.V3FundsDeposited.create_filter(from_block = start_block, argument_filters={'depositId' : deposit_id_array}).get_all_entries()

    contract = arb_spoke
    start_block = 245270149
    event_list = event_list + contract.events.V3FundsDeposited.create_filter(from_block = start_block, argument_filters={'depositId' : deposit_id_array}).get_all_entries()
    
    return event_list

# listen to the RelayedRootBundle event and filter the deposit_id to get the quoteTimestamp
def get_event_bundle_id(chain, relayer_root_array):
    contract = None
    start_block = 0
    chain = str(chain)
    if chain == "eth":
        contract = eth_spoke
        start_block = 20298487
    if chain == "op":
        contract = op_spoke
        start_block = 123750953
    elif chain == "base":
        contract = base_spoke
        start_block = 19043282
    elif chain == "arb":
        contract = arb_spoke
        start_block = 245270149

    event_list = contract.events.RelayedRootBundle.create_filter(from_block = start_block, argument_filters={'relayerRefundRoot' : relayer_root_array}
    ).get_all_entries()
    return event_list

def get_lp_fee(input_token, output_token, origin_chian_id, dest_chain_id, amount, timestamp):
    url = f"https://app.across.to/api/suggested-fees?inputToken={input_token}&outputToken={output_token}&originChainId={origin_chian_id}&destinationChainId={dest_chain_id}&amount={amount}&timestamp={timestamp}"
    print(url)
    try:
        response = requests.get(url)
        data = response.json()
        return data["lpFee"]["total"]
    except:
        return 0

def get_chain_id(chain):
    if chain == "eth":
        return 1
    if chain == "op":
        return 10
    if chain == "base":
        return 8453
    if chain == "arb":
        return 42161

def update_deposit_time():
    # get all deposit_id from the fill table
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT deposit_id, origin_chain, aim_chain, tx_hash, input_token, output_token, input_amount FROM Fill WHERE is_success = 1")
    result = cursor.fetchall()
    # put deposit_id into array
    deposit_id_list = []
    for row in result:
        deposit_id_list.append(int(row[0]))
    # get the deposit_event by the deposit_id array
    event_list = get_deposit_time(deposit_id_list)
    for row in result:
        for event in event_list:
            if int(row[0]) == int(event["args"]["depositId"]):
                lp_fee = get_lp_fee(row[4], row[5], row[1], get_chain_id(row[2]), row[6], event["args"]["quoteTimestamp"])
                cursor.execute(
                    "UPDATE Fill SET deposit_time = ?, lp_fee = ? WHERE tx_hash = ? AND aim_chain = ?",
                    (event["args"]["quoteTimestamp"], lp_fee, row[3], row[2]),
                )
                break;
    
    conn.commit()
    conn.close()
    return

def get_token_price(token, currency="usd"):
    key = os.getenv("COIN_KEY")
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={token}&vs_currencies={currency}&x_cg_demo_api_key={key}"
    print(url)
    response = requests.get(url)
    data = response.json()
    return data[token][currency]

def round_decimal(value, decimal=3):
    return round(value, decimal)