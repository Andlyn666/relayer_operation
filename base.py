import requests
import sqlite3
from web3 import Web3
import json
from dotenv import load_dotenv
import os

def decode_input_data(input_data, contract):
   decoded_data = contract.decode_function_input(input_data)
   return decoded_data

def insert_return_data(contract, cursor, web3, block=17987144):
    event_list = contract.events.ExecutedRelayerRefundRoot.create_filter(from_block=block).get_all_entries()
    print (event_list[0])
    for event in event_list:
      refund_addresses = event['args']['refundAddresses']
      address_to_check = '0x84A36d2C3d2078c560Ff7b62815138a16671b549'
      if address_to_check in refund_addresses:
         block = web3.eth.get_block(event['blockNumber'])
         # Get the timestamp from the block details
         timestamp = block['timestamp']
         indices = [i for i, addr in enumerate(refund_addresses) if addr == address_to_check]
         for index in indices:
            cursor.execute('''
                  INSERT INTO Return (
                     tx_hash, output_token, output_amount, aim_chain, block, time_stamp
                  ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                  event['transactionHash'].hex(), event['args']['l2TokenAddress'], event['args']['refundAmounts'][index],
                  'base', int(event['blockNumber']), timestamp
            ))
    return

def main():
    load_dotenv()
    abi = ''
    with open('spoke_abi.json', 'r', encoding='utf-8') as file:
         abi = json.load(file)
    base_rpc = os.getenv('BASE_RPC')
    web3 = Web3(Web3.HTTPProvider(base_rpc))
    # Define the contract address and create a contract instance
    contract_address = web3.to_checksum_address('0x09aea4b2242abc8bb4bb78d537a67a245a7bec64')
    contract = web3.eth.contract(address=contract_address, abi=abi)
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()

    # Get the last block number from Variable table
    cursor.execute('SELECT value FROM Variable WHERE name = ?', ('base_block',))
    result = cursor.fetchone()
    last_block = int(result[0]) + 1 if result else 17987144

    insert_return_data(contract, cursor, web3, last_block)
    #  print(last_block)
    base_key = os.getenv('BASE_KEY')
    # Define the URL with query parameters
    url = (
        f'https://api.basescan.org/api?module=account&action=txlist&address=0x84A36d2C3d2078c560Ff7b62815138a16671b549&startblock={last_block}&endblock=99999999&sort=asc&apikey={base_key}'
    )
    response = requests.get(url)
    data = response.json()
    for tx in data['result']:
      if tx['methodId'] == '0x2e378115':
         decode_input = decode_input_data(tx['input'], contract)
         decode_input = decode_input[1]['relayData']
         cursor.execute('''
               INSERT INTO Fill (
                  tx_hash, relayer, is_success, gas,
                  aim_chain, time_stamp, block, origin_chain,
                        input_amount, output_amount, deposit_id,
                        input_token, output_token
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ''', (
               tx['hash'], '0x84A36d2C3d2078c560Ff7b62815138a16671b549', tx['txreceipt_status'] == '1', int(tx['gasUsed']) * int(tx['gasPrice']),
               'base', int(tx['timeStamp']), int(tx['blockNumber']), decode_input['originChainId'],
               decode_input['inputAmount'], decode_input['outputAmount'], decode_input['depositId'],
               decode_input['inputToken'], decode_input['outputToken']
               
         ))

      # Insert last block number to LastBlock table, update if chain_name already exists
      cursor.execute('''
         INSERT INTO Variable (
               name, value
         ) VALUES (?, ?)
         ON CONFLICT(name) DO UPDATE SET value=excluded.value
        ''', (
         'base_block', int(tx['blockNumber'])
      ))

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()