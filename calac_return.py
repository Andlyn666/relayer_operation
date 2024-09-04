import requests
import sqlite3
from web3 import Web3
import json
import time
from decimal import Decimal

def get_pass_challenge_priod_block(chain):
    abi = ''
    with open('hub_abi.json', 'r', encoding='utf-8') as file:
        abi = json.load(file)
    web3 = Web3(Web3.HTTPProvider('https://eth-mainnet.g.alchemy.com/v2/pMl8wAjVvCRAzgqtijiX_4Wn60sat_m9'))
    contract_address = web3.to_checksum_address('0xc186fA914353c44b2E33eBE05f21846F1048bEda')
    contract = web3.eth.contract(address=contract_address, abi=abi)
    event_list = contract.events.ProposeRootBundle.create_filter(from_block=14819537).get_all_entries()

    for event in reversed(event_list):
        if event['args']['challengePeriodEndTimestamp'] < time.time():
            passed_block_event = event
            break
    if chain == "base":
        return passed_block_event['args']['bundleEvaluationBlockNumbers'][6]

def main():
    passed_block_base = get_pass_challenge_priod_block("base")
    print(passed_block_base)
    
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()
    
    # Calculate the total return amount and output token = ETH from the first record to the passed block of base by counting the return table
    cursor.execute('SELECT output_amount FROM Return WHERE block <= ? AND output_token = ?', (passed_block_base, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'))
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
    print("usdc return: " + str(total_return_amount))
    
    # Calculate the total output amount from the first record to the passed block of base by counting the fill table
    cursor.execute('SELECT output_amount FROM Fill WHERE block <= ? AND output_token = ? AND is_success = 1', (passed_block_base, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'))
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    print("usdc fill:   " + str(total_output_amount))

        # Calculate the total return amount and output token = ETH from the first record to the passed block of base by counting the return table
    cursor.execute('SELECT output_amount FROM Return WHERE block <= ? AND output_token = ?', (passed_block_base, '0x4200000000000000000000000000000000000006'))
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
    print("weth return: " + str(total_return_amount))
    
    # Calculate the total output amount from the first record to the passed block of base by counting the fill table
    cursor.execute('SELECT output_amount FROM Fill WHERE block <= ? AND output_token = ? AND is_success = 1', (passed_block_base, '0x4200000000000000000000000000000000000006'))
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    print("weth fill:   " + str(total_output_amount))

    cursor.execute('SELECT output_amount FROM Return WHERE block <= ?', (passed_block_base,))
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
    print("total return: " + str(total_return_amount))
    
    # Calculate the total output amount from the first record to the passed block of base by counting the fill table
    cursor.execute('SELECT output_amount FROM Fill WHERE block <= ? AND is_success = 1', (passed_block_base,))
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    print("total fill:   " + str(total_output_amount))
if __name__ == "__main__":
    main()