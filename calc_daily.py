import sqlite3
import time
from decimal import Decimal
from datetime import datetime
import pandas as pd
from tool import get_token_price, round_decimal, get_cex_fee


#calc total profit of each token in sum of each day
def calc_total_amount(cursor, output_token, token_name, chain, data):
    cursor.execute(
        "SELECT output_amount, input_amount, lp_fee, gas FROM Fill WHERE output_token = ? AND is_success = 1 AND aim_chain = ?",
        (output_token, chain),
    )
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    total_input_amount = sum(Decimal(amount[1]) for amount in fill_amounts)
    total_lp_fee = sum(Decimal(amount[2] or 0) for amount in fill_amounts)
    
    cursor.execute(
        "SELECT gas FROM Fill WHERE output_token = ? AND aim_chain = ? ",
        (output_token, chain),
    )
    gas_amounts = cursor.fetchall()
    total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
    total_gas_amount = total_gas_amount / 1000000000000000000

    profit_usd = 0
    gas_usd = 0
    lp_usd = 0
    if token_name == "usdc":
        total_output_amount = total_output_amount / 1000000
        total_input_amount = total_input_amount / 1000000
        total_lp_fee = total_lp_fee / 1000000
        if chain == "eth":
            total_lp_fee = total_lp_fee + Decimal(get_cex_fee('usdc', 0, time.time()))
        total_gas_amount = total_gas_amount * eth_price
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        
        profit_usd = Decimal(profit) * usdc_price
        lp_usd = Decimal(total_lp_fee) * usdc_price
        gas_usd = Decimal(total_gas_amount) * usdc_price

    if token_name == "weth":
        total_output_amount = total_output_amount / 1000000000000000000
        total_input_amount = total_input_amount / 1000000000000000000
        total_lp_fee = total_lp_fee / 1000000000000000000
        if chain == "eth":
            total_lp_fee = Decimal(get_cex_fee('eth', 0, time.time()))
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        
        profit_usd = Decimal(profit) * weth_price
        gas_usd = Decimal(total_gas_amount) * weth_price
        lp_usd = Decimal(total_lp_fee) * weth_price

    if token_name == "wbtc":
        total_output_amount = total_output_amount / 100000000
        total_input_amount = total_input_amount / 100000000
        total_lp_fee = total_lp_fee / 100000000

        total_gas_used = Decimal(total_gas_amount) / Decimal(wbtc_price_eth)
        profit = Decimal(total_input_amount) - Decimal(total_output_amount) - Decimal(total_gas_used) - Decimal(total_lp_fee)
        
        profit_usd = Decimal(profit) * wbtc_price
        gas_usd = Decimal(total_gas_used) * wbtc_price
        lp_usd = Decimal(total_lp_fee) * wbtc_price
    if token_name == "dai":
        total_output_amount = Decimal(total_output_amount / 1000000000000000000)
        total_input_amount = Decimal(total_input_amount / 1000000000000000000)
        total_lp_fee = Decimal(get_cex_fee('dai', 0, time.time()))
        total_gas_amount = Decimal(total_gas_amount) * eth_price
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        
        profit_usd = Decimal(profit) * dai_price
        gas_usd = Decimal(total_gas_amount) * dai_price
        lp_usd = Decimal(total_lp_fee) * dai_price
    if token_name == "usdt":
        total_output_amount = total_output_amount / 1000000
        total_input_amount = total_input_amount / 1000000
        total_lp_fee = Decimal(get_cex_fee('usdt', 0, time.time()))

        total_gas_amount = total_gas_amount * eth_price
        profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
        
        profit_usd = Decimal(profit) * usdt_price
        lp_usd = Decimal(total_lp_fee) * usdt_price
        gas_usd = Decimal(total_gas_amount) * usdt_price
    profit_usd = round_decimal(profit_usd)
    lp_usd = round_decimal(lp_usd)
    gas_usd = round_decimal(gas_usd)
    data.append([f'{chain}-{token_name}', profit_usd, lp_usd, gas_usd])
    return data

def calc_daily_count(cursor, output_token, token_name, chain):
    # get the total amount of the fill record of each day
    time_stamp = 1724860800
    print(f"Calculating Chain {chain} {token_name} Daily Count")
    data = []
    while time_stamp < time.time() + 86400:
        cursor.execute(
            "SELECT output_amount, input_amount, lp_fee, gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND is_success = 1 AND aim_chain = ?",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        total_input_amount = sum(Decimal(amount[1]) for amount in fill_amounts)
        total_lp_fee = sum(Decimal(amount[2] or 0) for amount in fill_amounts)
        cursor.execute(
            "SELECT gas FROM Fill WHERE time_stamp <= ? AND time_stamp >= ? AND output_token = ? AND aim_chain = ? ",
            (time_stamp, time_stamp - 86400, output_token, chain),
        )
        gas_amounts = cursor.fetchall()
        total_gas_amount = sum(Decimal(amount[0]) for amount in gas_amounts)
        total_gas_amount = Decimal(total_gas_amount / 1000000000000000000)

        total_order_number = len(gas_amounts)
        success_order_number = len(fill_amounts)

        profit_usd = 0
        lp_usd = 0
        gas_usd = 0
        date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")

        if token_name == "usdc":
            total_output_amount = Decimal(total_output_amount / 1000000)
            total_input_amount = Decimal(total_input_amount / 1000000)
            total_lp_fee = Decimal(total_lp_fee / 1000000)
            if chain == "eth":
                total_lp_fee = Decimal(get_cex_fee('usdc', time_stamp - 86400, time_stamp))
            total_gas_amount = Decimal(total_gas_amount * eth_price)
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * usdc_price
            lp_usd = Decimal(total_lp_fee) * usdc_price
            gas_usd = Decimal(total_gas_amount) * usdc_price
        if token_name == "dai":
            total_output_amount = Decimal(total_output_amount / 1000000000000000000)
            total_input_amount = Decimal(total_input_amount / 1000000000000000000)
            total_lp_fee = Decimal(get_cex_fee('dai', time_stamp - 86400, time_stamp))
            lp_usd = Decimal(total_lp_fee) * dai_price

            total_gas_amount = Decimal(total_gas_amount) * eth_price
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee

            profit_usd = Decimal(profit) * dai_price
            gas_usd = Decimal(total_gas_amount) * dai_price
        if token_name == "usdt":
            total_output_amount = Decimal(total_output_amount / 1000000)
            total_input_amount = Decimal(total_input_amount / 1000000)
            total_lp_fee = Decimal(get_cex_fee('usdt', time_stamp - 86400, time_stamp))

            total_gas_amount = Decimal(total_gas_amount * eth_price)
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * usdt_price
            lp_usd = Decimal(total_lp_fee) * usdt_price
            gas_usd = Decimal(total_gas_amount) * usdt_price
        if token_name == "weth":
            total_output_amount = Decimal(total_output_amount / 1000000000000000000)
            total_input_amount = Decimal(total_input_amount / 1000000000000000000)
            total_lp_fee = Decimal(total_lp_fee / 1000000000000000000)
            if chain == "eth":
                total_lp_fee = Decimal(get_cex_fee('eth', time_stamp - 86400, time_stamp))
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * weth_price
            lp_usd = Decimal(total_lp_fee) * weth_price
            gas_usd = Decimal(total_gas_amount) * weth_price

        if token_name == "wbtc":
            total_output_amount = Decimal(total_output_amount / 100000000)
            total_input_amount = Decimal(total_input_amount / 100000000)
            total_lp_fee = Decimal(total_lp_fee / 100000000)

            total_gas_amount = Decimal(total_gas_amount) / Decimal(wbtc_price_eth)
            profit = Decimal(total_input_amount) - Decimal(total_output_amount) - Decimal(total_gas_amount) - Decimal(total_lp_fee)
            
            profit_usd = Decimal(profit) * wbtc_price
            lp_usd = Decimal(total_lp_fee) * wbtc_price
            gas_usd = Decimal(total_gas_amount) * wbtc_price

        profit_usd = round_decimal(profit_usd)
        lp_usd = round_decimal(lp_usd)
        gas_usd = round_decimal(gas_usd)
        profit = round_decimal(profit)
        total_output_amount = round_decimal(total_output_amount)
        total_input_amount = round_decimal(total_input_amount)
        total_lp_fee = round_decimal(total_lp_fee, 5)
        total_gas_amount = round_decimal(total_gas_amount, 8)

        data.append([date_str, profit_usd, total_order_number, success_order_number, total_input_amount, total_output_amount, total_lp_fee, lp_usd, total_gas_amount, gas_usd])
        time_stamp += 86400
    df = pd.DataFrame(data, columns=["Date", "Profit(USD)", "Total Fill Orders", "Successful Orders", "Total Input Amount", "Total Output Amount", "Total LP Fee", "Total LP Fee(USD)","Total Gas Fee", "Total Gas Fee(USD)"])
    
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=f'{chain}_{token_name}', index=False)

    return time_stamp

def calc_total_profit(cursor):
    print("Calculating Total Profit")
    data = []
    data = calc_total_amount(cursor, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "usdc", "base", data)
    data = calc_total_amount(cursor, "0x4200000000000000000000000000000000000006", "weth", "base", data)
    data = calc_total_amount(cursor, "0x4200000000000000000000000000000000000006", "weth", "op", data)
    data = calc_total_amount(cursor, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1","weth", "arb", data)
    data = calc_total_amount(cursor, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599","wbtc", "eth", data)
    data = calc_total_amount(cursor, "0x6B175474E89094C44Da98b954EedeAC495271d0F","dai", "eth", data)
    data = calc_total_amount(cursor, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2","weth", "eth", data)
    data = calc_total_amount(cursor, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "usdc", "eth", data)
    data = calc_total_amount(cursor, "0xdAC17F958D2ee523a2206206994597C13D831ec7", "usdt", "eth", data)
    # Initialize sums for each column
    total_profit_usd = 0
    total_lp_usd = 0
    total_gas_usd = 0

    # Calculate sums for each column
    for row in data:
        total_profit_usd += row[1]
        total_lp_usd += row[2]
        total_gas_usd += row[3]

    # Add the totals row to the data
    data.append(["Total", total_profit_usd, total_lp_usd, total_gas_usd])

    df = pd.DataFrame(data, columns=["Token", "Profit(USD)", "Total LP Fee(USD)", "Total Gas Fee(USD)"])
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=f'total_profit', index=False)

def get_token_prices():
    global wbtc_price, weth_price, usdc_price, eth_price, wbtc_price_eth, dai_price, usdt_price
    wbtc_price = Decimal(get_token_price("wrapped-bitcoin"))
    weth_price = Decimal(get_token_price("weth"))
    usdc_price = Decimal(get_token_price("usd-coin"))
    eth_price = Decimal(get_token_price("ethereum"))
    wbtc_price_eth = Decimal(get_token_price("wrapped-bitcoin", "eth"))
    dai_price = Decimal(get_token_price("dai"))
    usdt_price = Decimal(get_token_price("tether"))

def calc_daily():
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    get_token_prices()
    calc_daily_count(
        cursor, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", "usdc", "base"
    )
    # insert the timestamp to the last block table
    calc_daily_count(
        cursor, "0x4200000000000000000000000000000000000006", "weth", "base"
    )
    calc_daily_count(cursor, "0x4200000000000000000000000000000000000006", "weth", "op")

    calc_daily_count(
        cursor, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1","weth", "arb"
    )
    calc_daily_count(
        cursor, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599","wbtc", "eth"
    )
    calc_daily_count(
        cursor, "0x6B175474E89094C44Da98b954EedeAC495271d0F","dai", "eth"
    )
    calc_daily_count(
        cursor, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2","weth", "eth"
    )
    calc_daily_count(
        cursor, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "usdc", "eth"
    )
    calc_daily_count (
        cursor, "0xdAC17F958D2ee523a2206206994597C13D831ec7", "usdt", "eth"
    )
    calc_total_profit(cursor)
    conn.close()
