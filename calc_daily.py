import sqlite3
import time
from decimal import Decimal
from datetime import datetime
import pandas as pd
from tool import get_token_price


#calc total profit of each token in sum of each day
def calc_total_amount(cursor, output_token, token_name, chain, data):
    cursor.execute(
        "SELECT output_amount, input_amount, lp_fee, gas FROM Fill WHERE output_token = ? AND is_success = 1 AND aim_chain = ?",
        (output_token, chain),
    )
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    total_input_amount = sum(Decimal(amount[1]) for amount in fill_amounts)
    total_lp_fee = sum(Decimal(amount[2]) for amount in fill_amounts)
    cursor.execute(
        "SELECT gas FROM Fill WHERE output_token = ? AND aim_chain = ? ",
        (output_token, chain),
    )
    gas_amounts = cursor.fetchall()
    total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
    total_gas_amount = total_gas_amount / 1000000000000000000

    token_price = 0
    profit_usd = 0
    if token_name == "usdc":
        token_price = Decimal(get_token_price("usd-coin"))
        token_price_eth = Decimal(get_token_price("ethereum"))
        total_output_amount = total_output_amount / 1000000
        total_input_amount = total_input_amount / 1000000
        total_lp_fee = total_lp_fee / 1000000
        total_gas_amount = total_gas_amount * token_price_eth
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        profit_usd = Decimal(profit) * token_price
    if token_name == "weth":
        token_price = Decimal(get_token_price("weth"))
        total_output_amount = total_output_amount / 1000000000000000000
        total_input_amount = total_input_amount / 1000000000000000000
        total_lp_fee = total_lp_fee / 1000000000000000000
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        profit_usd = Decimal(profit) * token_price
    if token_name == "wbtc":
        token_price = Decimal(get_token_price("wrapped-bitcoin"))
        total_output_amount = total_output_amount / 100000000
        total_input_amount = total_input_amount / 100000000
        total_lp_fee = total_lp_fee / 100000000
        total_gas_amount = Decimal(total_gas_amount) / Decimal(24.57)
        profit = Decimal(total_input_amount) - Decimal(total_output_amount) - Decimal(total_gas_amount) - Decimal(total_lp_fee)
        profit_usd = Decimal(profit) * token_price
    data.append([f'{chain}-{token_name}', profit_usd, total_lp_fee, total_gas_amount])
    return data

def calc_daily_count(cursor, output_token, token_name, chain):
    # get the total amount of the fill record of each day
    time_stamp = 1724860800
    # # select time_stamp from variable table if exists
    # cursor.execute('SELECT value FROM Variable WHERE name = ?', ('last_time',))
    # result = cursor.fetchone()
    # if result:
    #     time_stamp = int(result[0])
    print(f"Chain {chain} {token_name} :")
    data = []
    while time_stamp < time.time():
        cursor.execute(
            "SELECT output_amount, input_amount, lp_fee, gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND is_success = 1 AND aim_chain = ?",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        total_input_amount = sum(Decimal(amount[1]) for amount in fill_amounts)
        total_lp_fee = sum(Decimal(amount[2]) for amount in fill_amounts)
        cursor.execute(
            "SELECT gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND aim_chain = ? ",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        gas_amounts = cursor.fetchall()
        total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
        total_gas_amount = total_gas_amount / 1000000000000000000

        total_order_number = len(gas_amounts)
        success_order_number = len(fill_amounts)
        token_price = 0
        profit_usd = 0
        if token_name == "usdc":
            token_price = Decimal(get_token_price("usd-coin"))
            token_price_eth = Decimal(get_token_price("ethereum"))
            total_output_amount = total_output_amount / 1000000
            total_input_amount = total_input_amount / 1000000
            total_lp_fee = total_lp_fee / 1000000
            total_gas_amount = total_gas_amount * token_price_eth
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            profit_usd = Decimal(profit) * token_price
            date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")
            print(
                f"{date_str}  profit: {profit} USD fill order: {total_order_number} success order: {success_order_number}"
            )
        if token_name == "weth":
            token_price = Decimal(get_token_price("weth"))
            total_output_amount = total_output_amount / 1000000000000000000
            total_input_amount = total_input_amount / 1000000000000000000
            total_lp_fee = total_lp_fee / 1000000000000000000
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            profit_usd = Decimal(profit) * token_price
            date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")
            print(
                f"{date_str}  profit: {profit} ETH fill order: {total_order_number} success order: {success_order_number}"
            )
        if token_name == "wbtc":
            token_price = Decimal(get_token_price("wrapped-bitcoin"))
            total_output_amount = total_output_amount / 100000000
            total_input_amount = total_input_amount / 100000000
            total_lp_fee = total_lp_fee / 100000000
            total_gas_amount = Decimal(total_gas_amount) / Decimal(24.57)
            profit = Decimal(total_input_amount) - Decimal(total_output_amount) - Decimal(total_gas_amount) - Decimal(total_lp_fee)
            profit_usd = Decimal(profit) * token_price
            date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")
            print(
                f"{date_str}  profit: {profit} BTC fill order: {total_order_number} success order: {success_order_number}"
            )
        data.append([date_str, profit_usd, total_order_number, success_order_number, total_input_amount, total_output_amount, total_lp_fee, total_gas_amount])
        time_stamp += 86400
    df = pd.DataFrame(data, columns=["Date", "USD Profit", "Total Fill Orders", "Successful Orders", "Total Input Amount", "Total Output Amount", "Total LP Fee", "Total Gas Fee"])
    
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=f'{chain}_{token_name}', index=False)

    return time_stamp

def calc_total_profit(cursor):
    data = []
    data = calc_total_amount(cursor, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "usdc", "base", data)
    data = calc_total_amount(cursor, "0x4200000000000000000000000000000000000006", "weth", "base", data)
    data = calc_total_amount(cursor, "0x4200000000000000000000000000000000000006", "weth", "op", data)
    data = calc_total_amount(cursor, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1","weth", "arb", data)
    data = calc_total_amount(cursor, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599","wbtc", "eth", data)
    df = pd.DataFrame(data, columns=["Token", "USD Profit", "Total LP Fee", "Total Gas Fee"])
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=f'total_profit', index=False)

def calc_daily():
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    # calc_daily_count(
    #     cursor, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "usdc", "base"
    # )
    # # insert the timestamp to the last block table
    # calc_daily_count(
    #     cursor, "0x4200000000000000000000000000000000000006", "weth", "base"
    # )
    # calc_daily_count(cursor, "0x4200000000000000000000000000000000000006", "weth", "op")

    # calc_daily_count(
    #     cursor, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1","weth", "arb"
    # )
    # calc_daily_count(
    #     cursor, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599","wbtc", "eth"
    # )
    calc_total_profit(cursor)
    conn.close()