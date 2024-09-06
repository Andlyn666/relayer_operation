import sqlite3
from web3 import Web3
import json
import time
from decimal import Decimal
from dotenv import load_dotenv
import os
from datetime import datetime

def get_pass_challenge_priod_block(chain):
    abi = ''
    with open('hub_abi.json', 'r', encoding='utf-8') as file:
        abi = json.load(file)
    eth_rpc = os.getenv('ETH_RPC')
    web3 = Web3(Web3.HTTPProvider(eth_rpc))
    contract_address = web3.to_checksum_address('0xc186fA914353c44b2E33eBE05f21846F1048bEda')
    contract = web3.eth.contract(address=contract_address, abi=abi)
    event_list = contract.events.ProposeRootBundle.create_filter(from_block=14819537).get_all_entries()

    for event in reversed(event_list):
        if event['args']['challengePeriodEndTimestamp'] < time.time():
            passed_block_event = event;
            break
    if chain == "base":
        return passed_block_event['args']['bundleEvaluationBlockNumbers'][6]

def calc_total_amount(cursor, passed_block, output_token, token_name):
    
    cursor.execute('SELECT output_amount FROM Return WHERE output_token = ?', (output_token,))
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
    
    cursor.execute('SELECT output_amount FROM Fill WHERE block <= ? AND output_token = ? AND is_success = 1', (passed_block, output_token))
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)

    cursor.execute('SELECT gas FROM Fill WHERE block <= ? AND output_token = ?', (passed_block, output_token))
    gas_amounts = cursor.fetchall()
    total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
    total_gas_amount = total_gas_amount / 1000000000000000000
    
    if (token_name == 'usdc'):
        total_return_amount = total_return_amount / 1000000
        total_output_amount = total_output_amount / 1000000
        # claculate the gas fee of all fill transactions and using the renturn - fill - total gas to get the profit
        total_usd = total_gas_amount * 2400
        profit = total_return_amount - total_output_amount - total_usd
        print(f"{token_name} return: {total_return_amount} fill: {total_output_amount}  profit: {profit} about {profit} USD")
    
    if (token_name == 'weth'):
        total_return_amount = total_return_amount / 1000000000000000000
        total_output_amount = total_output_amount / 1000000000000000000
        profit = total_return_amount - total_output_amount - total_gas_amount
        print(f"{token_name} return: {total_return_amount} fill: {total_output_amount}  profit: {profit} about {profit * 2400} USD")


def calc_daily_count(cursor, output_token, token_name):
    # get the total amount of the fill record of each day
    time_stamp = 1724860800
    # # select time_stamp from variable table if exists
    # cursor.execute('SELECT value FROM Variable WHERE name = ?', ('last_time',))
    # result = cursor.fetchone()
    # if result:
    #     time_stamp = int(result[0])
    while time_stamp < time.time():
        cursor.execute('SELECT output_amount FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND is_success = 1', (time_stamp, time_stamp - 86400, output_token))
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)

        cursor.execute('SELECT gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ?', (time_stamp, time_stamp - 86400, output_token))
        gas_amounts = cursor.fetchall()
        total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
        total_gas_amount = total_gas_amount / 1000000000000000000

        total_order_number = len(gas_amounts)
        success_order_number = len(fill_amounts)
        cursor.execute('SELECT output_amount FROM Return WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ?', (time_stamp, time_stamp - 86400, output_token))
        return_amounts = cursor.fetchall()
        total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
        if (token_name == 'usdc'):
            total_output_amount = total_output_amount / 1000000
            total_return_amount = total_return_amount / 1000000
            total_usd = total_gas_amount * 2400
            date_str = datetime.fromtimestamp(time_stamp).strftime('%Y%m%d')
            print(f"{date_str} {token_name} total order: {total_order_number} success order: {success_order_number}")
        if (token_name == 'weth'):
            total_output_amount = total_output_amount / 1000000000000000000
            total_return_amount = total_return_amount / 1000000000000000000
            date_str = datetime.fromtimestamp(time_stamp).strftime('%Y%m%d')
            print(f"{date_str} {token_name} total order: {total_order_number} success order: {success_order_number}")
        time_stamp += 86400
    return time_stamp


def main():
    load_dotenv()
    passed_block_base = get_pass_challenge_priod_block("base")
    
    conn = sqlite3.connect('mydatabase.db')
    cursor = conn.cursor()
    
    calc_total_amount(cursor, passed_block_base, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', 'usdc')
    calc_total_amount(cursor, passed_block_base, '0x4200000000000000000000000000000000000006', 'weth')
    time_stamp = calc_daily_count(cursor, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', 'usdc')
    # insert the timestamp to the last block table
    time_stamp = calc_daily_count(cursor, '0x4200000000000000000000000000000000000006', 'weth')
    cursor.execute('''
        INSERT INTO Variable (
            name, value
        ) VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET value=excluded.value
    ''', (
        'last_time', time_stamp
    ))

if __name__ == "__main__":
    main()