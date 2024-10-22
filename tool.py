import os
from web3 import Web3
import json
import sqlite3
import requests
import base64
import time
import urllib
import hashlib
import hmac
from binance.spot import Spot as Client
from dotenv import load_dotenv
from datetime import datetime

def get_variable(name):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM Variable WHERE name = ?", (name,))
    result = cursor.fetchone()
    conn.close()
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
    conn.close()

def update_bundle(chain, start_block):
    abi = ""
    with open("hub_abi.json", "r", encoding="utf-8") as file:
        abi = json.load(file)
    eth_rpc = os.getenv("ETH_RPC")
    web3 = Web3(Web3.HTTPProvider(eth_rpc))
    contract_address = web3.to_checksum_address(
        "0xc186fA914353c44b2E33eBE05f21846F1048bEda"
    )
    contract = web3.eth.contract(address=contract_address, abi=abi)
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    last_bundle_id = get_variable(f"last_{chain}_bundle_id")
    last_block = get_block_by_bundle_id(last_bundle_id, chain, cursor) + 1
    if last_block > start_block:
        start_block = last_block
    propose_list = contract.events.ProposeRootBundle.create_filter(
        from_block=14819537
    ).get_all_entries()
    relayer_root_list = []
    for event in propose_list:
        relayer_root_list.append(event["args"]["relayerRefundRoot"])
    bundle_event_list = get_event_bundle_id(chain, relayer_root_list, cursor)
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
                            chain, bundle_id, refund_root, base_end_block, op_end_block, arb_end_block, eth_end_block
                            ) VALUES (?, ?, ?, ?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                event["args"]["relayerRefundRoot"],
                                bundle_numbers[6],
                                bundle_numbers[1],
                                bundle_numbers[4],
                                bundle_numbers[0],
                            ),
                        )
                if chain == "op" and len(bundle_numbers) >= 2:
                    if bundle_numbers[1] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, refund_root, base_end_block, op_end_block, arb_end_block, eth_end_block
                            ) VALUES (?, ?, ?, ?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                event["args"]["relayerRefundRoot"],
                                bundle_numbers[6],
                                bundle_numbers[1],
                                bundle_numbers[4],
                                bundle_numbers[0],
                            ),
                        )
                if chain == "arb" and len(bundle_numbers) >= 5:
                    if bundle_numbers[4] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, refund_root, base_end_block, op_end_block, arb_end_block, eth_end_block
                            ) VALUES (?, ?, ?, ?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                event["args"]["relayerRefundRoot"],
                                bundle_numbers[6],
                                bundle_numbers[1],
                                bundle_numbers[4],
                                bundle_numbers[0],
                            ),
                        )
                if chain == "eth" and len(bundle_numbers) >= 1:
                    if bundle_numbers[0] >= start_block:
                        cursor.execute(
                            """
                            INSERT INTO Bundle (
                            chain, bundle_id, refund_root, base_end_block, op_end_block, arb_end_block, eth_end_block
                            ) VALUES (?, ?, ?, ?, ?, ?, ?) 
                            """,
                            (
                                chain,
                                bundle_id,
                                event["args"]["relayerRefundRoot"],
                                bundle_numbers[6],
                                bundle_numbers[1],
                                bundle_numbers[4],
                                bundle_numbers[0],
                            ),
                        )
                break
    conn.commit()
    conn.close()
    update_variable(f"last_{chain}_bundle_id", bundle_id)


def get_bundle_id(block, cursor, aim_chain, repayment_chain):

    repayment_chain_name = get_chain_name(int(repayment_chain))
    cursor.execute(
        f"SELECT bundle_id FROM Bundle WHERE {aim_chain}_end_block > ? AND chain = ?",
        (block, repayment_chain_name),
    )
    result = cursor.fetchall()
    if not result:
        return 0
    bundle_id = int(result[0][0])
    return bundle_id


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

def get_block_by_bundle_id(bundle_id, chain, cursor):
    cursor.execute(
        f"SELECT {chain}_end_block FROM Bundle WHERE bundle_id = ? AND chain = ?",
        (bundle_id, chain),
    )
    result = cursor.fetchall()
    if result:
        return result[0][0]
    return 0
# listen to the RelayedRootBundle event and filter the deposit_id to get the quoteTimestamp
def get_event_bundle_id(chain, relayer_root_array, cursor):
    contract = None
    start_block = 0
    chain = str(chain)
    last_bundle_id = get_variable(f"last_{chain}_bundle_id")
    start_block= get_block_by_bundle_id(last_bundle_id, chain, cursor) + 1
    print(f"start_block: {start_block}")
    if chain == "eth":
        contract = eth_spoke
        start_block = 20298487 if start_block == 0 else start_block
    if chain == "op":
        contract = op_spoke
        start_block = 123750953 if start_block == 0 else start_block
    elif chain == "base":
        contract = base_spoke
        start_block = 19043282 if start_block == 0 else start_block
    elif chain == "arb":
        contract = arb_spoke
        start_block = 245270149 if start_block == 0 else start_block

    event_list = contract.events.RelayedRootBundle.create_filter(from_block = start_block, argument_filters={'relayerRefundRoot' : relayer_root_array}
    ).get_all_entries()
    return event_list

def get_lp_fee(input_token, output_token, origin_chian_id, dest_chain_id, amount, timestamp):
    url = f"https://app.across.to/api/suggested-fees?inputToken={input_token}&outputToken={output_token}&originChainId={origin_chian_id}&destinationChainId={dest_chain_id}&amount={amount}&timestamp={timestamp}"
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

def get_chain_name(chain_id):
    if chain_id == 1:
        return "eth"
    if chain_id == 10:
        return "op"
    if chain_id == 8453:
        return "base"
    if chain_id == 42161:
        return "arb"

def update_deposit_time():
    # get all deposit_id from the fill table
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute('''
    UPDATE Fill
    SET lp_fee = 0
    WHERE repayment_chain = origin_chain
    ''')
    cursor.execute("SELECT deposit_id, origin_chain, aim_chain, tx_hash, input_token, output_token, input_amount FROM Fill WHERE is_success = 1 AND deposit_time is NULL AND repayment_chain != origin_chain")
    result = cursor.fetchall()
    # put deposit_id into array
    deposit_id_list = []
    for row in result:
        deposit_id_list.append(int(row[0]))
    if len(deposit_id_list) == 0:
        return
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
                break
    
    conn.commit()
    conn.close()
    return

def get_token_price(token, currency="usd"):
    key = os.getenv("COIN_KEY")
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={token}&vs_currencies={currency}&x_cg_demo_api_key={key}"
    response = requests.get(url)
    data = response.json()
    return data[token][currency]

def round_decimal(value, decimal=3):
    return round(value, decimal)

def get_cex_fee_results(token, start_time, end_time):
    if token == 'dai':
        api_domain = "https://api.kraken.com"
        api_method = "WithdrawStatus"
        api_data = f"asset={token}&start={start_time}&end={end_time}"
        api_path = "/0/private/"
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = base64.b64decode(os.getenv("KRAKEN_API_SECRET"))
        api_nonce = str(int(time.time() * 1000))
        api_postdata = api_data + "&nonce=" + api_nonce
        api_postdata = api_postdata.encode("utf-8")
        api_sha256 = hashlib.sha256(api_nonce.encode("utf-8") + api_postdata).digest()
        api_hmacsha512 = hmac.new(
            api_secret,
            api_path.encode("utf-8") + api_method.encode("utf-8") + api_sha256,
            hashlib.sha512,
        )
        api_request = urllib.request.Request(
            api_domain + api_path + api_method, api_postdata
        )
        api_request.add_header("API-Key", api_key)
        api_request.add_header("API-Sign", base64.b64encode(api_hmacsha512.digest()))
        api_request.add_header("User-Agent", "Kraken REST API")
        try:
            api_reply = urllib.request.urlopen(api_request).read()
        except Exception as error:
            print("API call failed")
            return None
        try:
            api_reply = api_reply.decode()
        except Exception as error:
            print("API reply decode failed")
            return None
        result = json.loads(api_reply)
        if result.keys() != {"error", "result"}:
            print(result)
            return 0
        result = result['result']
        return result
    if token == 'weth':
        try:
            his_withdraw = []
            api_key = os.getenv("BINANCE_API_KEY")
            api_secret = os.getenv("BINANCE_API_SECRET")
            client = Client(api_key, api_secret)
            start_timestamp = int(start_time * 1000)
            end_timestamp = int(end_time * 1000)
            his_withdraw = client.withdraw_history(startTime=start_timestamp, endTime=end_timestamp, status='6', coin='ETH')
            return his_withdraw

        except Exception as e:
            print(f"Binance API exception occurred: {e}")
            print(f"start_time: {start_time}, end_time: {end_time}")
            return []
    
def convert_to_timestamp(time_str):
    dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    return int(dt.timestamp())

# fetch the fee by the get_cex_fee_results function and insert the result into the CEX_FEE table
def update_cex_fee():
    timestamp = int(time.time())
    # get the last cex fee time stamp from the Variable table
    last_cex_fee_time_stamp = get_variable("last_cex_fee_time_stamp_binance")
    # get the cex fee by the get_cex_fee_results function
    if last_cex_fee_time_stamp == 1:
        last_cex_fee_time_stamp = timestamp - 86400 * 60
    cex_fee_binance = []
    if timestamp - int(last_cex_fee_time_stamp) > 86400:
        cex_fee_binance = get_cex_fee_results('weth', last_cex_fee_time_stamp, timestamp)
    # insert the cex fee into the CEX_FEE table
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    for tx in cex_fee_binance:
        cursor.execute("""
            INSERT OR IGNORE INTO CEX_FEE (token, chain, fee, time_stamp) 
            VALUES (?, ?, ?, ?)
        """, ('weth', 'eth', tx['transactionFee'], convert_to_timestamp(tx['completeTime'])))
    conn.commit()
    conn.close()
    update_variable("last_cex_fee_time_stamp_binance", timestamp)

    last_cex_fee_time_stamp = get_variable("last_cex_fee_time_stamp_kraken")
    if last_cex_fee_time_stamp == 1:
        last_cex_fee_time_stamp = timestamp - 86400 * 60
    cex_fee_kraken = get_cex_fee_results('dai', last_cex_fee_time_stamp, timestamp)
    #insert the cex fee into the CEX_FEE table
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    for tx in cex_fee_kraken:
        cursor.execute("INSERT OR IGNORE INTO CEX_FEE (token, chain, fee, time_stamp) VALUES (?, ?, ?, ?)", ('dai', 'eth', tx['fee'], tx['time']))
    conn.commit()
    conn.close()
    update_variable("last_cex_fee_time_stamp_kraken", timestamp)

# get sum of the fee from the CEX_FEE table
def get_cex_fee(token, start_time, end_time):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(fee) FROM CEX_FEE WHERE token = ? AND time_stamp >= ? AND time_stamp <= ?", (token, start_time, end_time))
    result = cursor.fetchone()
    conn.close()
    ret = result[0] if result else 0
    if ret is None:
        ret = 0
    return ret

def main():
    load_dotenv()
    # get current timestamp
    timestamp = int(time.time())
    # get timestamp 1 day ago
    timestamp_yesterday = timestamp - 86400 * 20
    #get_cex_fee_results('weth', timestamp_yesterday, timestamp)
    update_cex_fee()
    print(get_cex_fee('weth', timestamp_yesterday, timestamp))

if __name__ == "__main__":
    main()
