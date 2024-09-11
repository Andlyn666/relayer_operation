import sqlite3
import time
from decimal import Decimal
from dotenv import load_dotenv
from datetime import datetime
from base import update_base, get_latest_base_block
from op import update_op, get_latest_op_block



def calc_total_amount(
    cursor, passed_block, output_token, token_name, chain, latest_block
):

    cursor.execute(
        "SELECT output_amount FROM Return WHERE output_token = ? AND aim_chain = ? AND block <= ?",
        (output_token, chain, latest_block),
    )
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)

    cursor.execute(
        "SELECT output_amount FROM Fill WHERE block <= ? AND output_token = ? AND is_success = 1 AND aim_chain = ?",
        (passed_block, output_token, chain),
    )
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)

    cursor.execute(
        "SELECT gas FROM Fill WHERE block <= ? AND output_token = ? AND aim_chain = ?",
        (passed_block, output_token, chain),
    )
    gas_amounts = cursor.fetchall()
    total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
    total_gas_amount = total_gas_amount / 1000000000000000000

    if token_name == "usdc":
        total_return_amount = total_return_amount / 1000000
        total_output_amount = total_output_amount / 1000000
        # claculate the gas fee of all fill transactions and using the renturn - fill - total gas to get the profit
        total_usd = total_gas_amount * 2400
        profit = total_return_amount - Decimal(total_output_amount) - Decimal(total_usd)
        print(
            f"{chain} {token_name} return: {total_return_amount} fill: {total_output_amount}  profit: {profit} about {profit} USD"
        )

    if token_name == "weth":
        total_return_amount = total_return_amount / 1000000000000000000
        total_output_amount = total_output_amount / 1000000000000000000
        profit = (
            total_return_amount
            - Decimal(total_output_amount)
            - Decimal(total_gas_amount)
        )
        print(
            f"{chain} {token_name} return: {total_return_amount} fill: {total_output_amount}  profit: {profit} about {profit * 2400} USD"
        )


def calc_daily_count(cursor, output_token, token_name, chain):
    # get the total amount of the fill record of each day
    time_stamp = 1724860800
    # # select time_stamp from variable table if exists
    # cursor.execute('SELECT value FROM Variable WHERE name = ?', ('last_time',))
    # result = cursor.fetchone()
    # if result:
    #     time_stamp = int(result[0])
    print(f"Chain {chain} {token_name} :")
    while time_stamp < time.time():
        cursor.execute(
            "SELECT output_amount, input_amount FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND is_success = 1 AND aim_chain = ?",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        total_input_amount = sum(Decimal(amount[1]) for amount in fill_amounts)
        cursor.execute(
            "SELECT gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND aim_chain = ? ",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        gas_amounts = cursor.fetchall()
        total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
        total_gas_amount = total_gas_amount / 1000000000000000000

        total_order_number = len(gas_amounts)
        success_order_number = len(fill_amounts)
        if token_name == "usdc":
            total_output_amount = total_output_amount / 1000000
            total_input_amount = total_input_amount / 1000000
            total_gas_usd = total_gas_amount * 2400
            profit = total_input_amount - total_output_amount - total_gas_usd
            date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")
            print(
                f"{date_str}  profit: {profit} USD fill order: {total_order_number} success order: {success_order_number}"
            )
        if token_name == "weth":
            total_output_amount = total_output_amount / 1000000000000000000
            total_input_amount = total_input_amount / 1000000000000000000
            profit = total_input_amount - total_output_amount - total_gas_amount
            date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")
            print(
                f"{date_str}  profit: {profit} ETH fill order: {total_order_number} success order: {success_order_number}"
            )
        time_stamp += 86400
    return time_stamp


def calc_daily():
    update_base()
    update_op()
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    calc_daily_count(
        cursor, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "usdc", "base"
    )
    # insert the timestamp to the last block table
    calc_daily_count(
        cursor, "0x4200000000000000000000000000000000000006", "weth", "base"
    )

    calc_daily_count(cursor, "0x4200000000000000000000000000000000000006", "weth", "op")
    conn.close()