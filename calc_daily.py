import sqlite3
import time
from decimal import Decimal
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from tool import get_token_price, round_decimal, get_cex_fee, create_w3_contract, update_cex_fee
from upload_file import upload_to_gdrive
from calc_apy import calc_apy

def get_total_metrics(chain, token_name, data):
    try:
        # Read data from Excel sheet
        df = pd.read_excel('daily_count.xlsx', sheet_name=f'{chain}_{token_name}')
        
        # Calculate totals
        total_profit = df['Profit(USD)'].sum()
        total_lp = df['Total LP Fee(USD)'].sum()
        total_gas = df['Total Gas Fee(USD)'].sum()
        
        # Create new row with totals
        data.append([f'{chain}-{token_name}', total_profit, total_lp, total_gas])
        return data
        
    except Exception as e:
        print(f"Error calculating totals: {str(e)}")
        return data

def calc_daily_count(cursor, output_token, token_name, chain):
    print(f"Calculating Chain {chain} {token_name} Daily Count")
    
    # Initialize start timestamp
    time_stamp = 1724860800
    existing_df = pd.DataFrame()
    
    # Try to read existing file and get last date
    try:
        existing_df = pd.read_excel('daily_count.xlsx', sheet_name=f'{chain}_{token_name}')
        if not existing_df.empty:
            last_date = existing_df['Date'].max()
            # Convert YYYYMMDD string to timestamp
            time_stamp = int(datetime.strptime(str(last_date), "%Y%m%d").timestamp())
            print(f"Last date found: {last_date}")
    except (FileNotFoundError, ValueError, KeyError):
        # File doesn't exist or sheet not found - use default timestamp
        pass
    data = []
   
    # Calculate new data
    while time_stamp < time.time():
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
        total_gas_eth = total_gas_amount

        total_order_number = len(gas_amounts)
        success_order_number = len(fill_amounts)
        date_str_to_get_coin = datetime.fromtimestamp(time_stamp).strftime("%d-%m-%Y")

        profit_usd = 0
        lp_usd = 0
        gas_usd = 0
        token_price = 0
        eth_price = Decimal(get_token_price("ethereum", date_str_to_get_coin))

        date_str = datetime.fromtimestamp(time_stamp).strftime("%Y%m%d")

        if token_name == "usdc":
            token_price = Decimal(get_token_price("usd-coin", date_str_to_get_coin))
            total_output_amount = Decimal(total_output_amount / 1000000)
            total_input_amount = Decimal(total_input_amount / 1000000)
            total_lp_fee = Decimal(total_lp_fee / 1000000)
            if chain == "eth":
                total_lp_fee = Decimal(get_cex_fee('usdc', time_stamp - 86400, time_stamp))
            total_gas_amount = Decimal(total_gas_amount * eth_price)
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * token_price
            lp_usd = Decimal(total_lp_fee) * token_price
            gas_usd = Decimal(total_gas_amount) * token_price
        if token_name == "dai":
            token_price = Decimal(get_token_price("dai", date_str_to_get_coin))
            total_output_amount = Decimal(total_output_amount / 1000000000000000000)
            total_input_amount = Decimal(total_input_amount / 1000000000000000000)
            total_lp_fee = Decimal(get_cex_fee('dai', time_stamp - 86400, time_stamp))
            lp_usd = Decimal(total_lp_fee) * token_price

            total_gas_amount = Decimal(total_gas_amount) * eth_price
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee

            profit_usd = Decimal(profit) * token_price
            gas_usd = Decimal(total_gas_amount) * token_price
        if token_name == "usdt":
            token_price = Decimal(get_token_price("tether", date_str_to_get_coin))
            total_output_amount = Decimal(total_output_amount / 1000000)
            total_input_amount = Decimal(total_input_amount / 1000000)
            total_lp_fee = Decimal(get_cex_fee('usdt', time_stamp - 86400, time_stamp))

            total_gas_amount = Decimal(total_gas_amount * eth_price)
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * token_price
            lp_usd = Decimal(total_lp_fee) * token_price
            gas_usd = Decimal(total_gas_amount) * token_price
        if token_name == "weth":
            token_price = Decimal(get_token_price("weth", date_str_to_get_coin))
            total_output_amount = Decimal(total_output_amount / 1000000000000000000)
            total_input_amount = Decimal(total_input_amount / 1000000000000000000)
            total_lp_fee = Decimal(total_lp_fee / 1000000000000000000)
            if chain == "eth":
                total_lp_fee = Decimal(get_cex_fee('eth', time_stamp - 86400, time_stamp))
            profit = total_input_amount - total_output_amount - total_gas_amount - total_lp_fee
            
            profit_usd = Decimal(profit) * token_price
            lp_usd = Decimal(total_lp_fee) * token_price
            gas_usd = Decimal(total_gas_amount) * token_price

        if token_name == "wbtc":
            token_price = Decimal(get_token_price("wrapped-bitcoin", date_str_to_get_coin))
            wbtc_price_eth = Decimal(get_token_price("wrapped-bitcoin", date_str_to_get_coin, "eth"))
            total_output_amount = Decimal(total_output_amount / 100000000)
            total_input_amount = Decimal(total_input_amount / 100000000)
            total_lp_fee = Decimal(total_lp_fee / 100000000)

            total_gas_amount = Decimal(total_gas_amount) / Decimal(wbtc_price_eth)
            profit = Decimal(total_input_amount) - Decimal(total_output_amount) - Decimal(total_gas_amount) - Decimal(total_lp_fee)
            
            profit_usd = Decimal(profit) * token_price
            lp_usd = Decimal(total_lp_fee) * token_price
            gas_usd = Decimal(total_gas_amount) * token_price

        profit_usd = round_decimal(profit_usd)
        lp_usd = round_decimal(lp_usd)
        gas_usd = round_decimal(gas_usd)
        profit = round_decimal(profit)
        total_output_amount = round_decimal(total_output_amount)
        total_input_amount = round_decimal(total_input_amount)
        total_lp_fee = round_decimal(total_lp_fee, 5)
        total_gas_amount = round_decimal(total_gas_amount, 8)

        data.append([date_str, profit_usd, total_order_number, success_order_number, 
                    total_input_amount, total_output_amount, total_lp_fee, lp_usd, 
                    total_gas_amount, gas_usd, total_gas_eth, token_price, eth_price])
        time_stamp += 86400

    # Create DataFrame with new data
    new_df = pd.DataFrame(data, columns=["Date", "Profit(USD)", "Total Fill Orders", 
                         "Successful Orders", "Total Input Amount", "Total Output Amount", 
                         "Total LP Fee", "Total LP Fee(USD)","Total Gas Fee", 
                         "Total Gas Fee(USD)", "Total Gas Fee(ETH)", "Token Price", "ETH Price"])
    try:
        existing_df = pd.read_excel('daily_count.xlsx', sheet_name=f'{chain}_{token_name}')
        if existing_df.empty:
            final_df = new_df
        elif new_df.empty:
            final_df = existing_df
        else:
            # Convert dates to consistent format
            existing_df['Date'] = pd.to_datetime(existing_df['Date'], format='%Y%m%d').dt.strftime('%Y%m%d')
            new_df['Date'] = pd.to_datetime(new_df['Date'], format='%Y%m%d').dt.strftime('%Y%m%d')
            
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            final_df = combined_df.drop_duplicates(subset=['Date'], keep='last')
            # Sort using string comparison since dates are in YYYYMMDD format
            final_df = final_df.sort_values('Date', ascending=True)
    except:
        # If no existing data, use new data
        final_df = new_df

    # Write to Excel
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', 
                       if_sheet_exists='replace') as writer:
        final_df.to_excel(writer, sheet_name=f'{chain}_{token_name}', index=False)

    return time_stamp

def calc_total_profit(cursor):
    print("Calculating Total Profit")
    data = []
    data = get_total_metrics('base', 'usdc', data)
    data = get_total_metrics('base', 'weth', data)
    data = get_total_metrics('op', 'weth', data)
    data = get_total_metrics('arb', 'weth', data)
    data = get_total_metrics('eth', 'wbtc', data)
    data = get_total_metrics('eth', 'dai', data)
    data = get_total_metrics('eth', 'weth', data)
    data = get_total_metrics('eth', 'usdc', data)
    data = get_total_metrics('eth', 'usdt', data)
    data = get_total_metrics('arb', 'usdc', data)
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


def calc_daily():
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
    calc_daily_count(
        cursor, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "usdc", "arb"
    )
    calc_total_profit(cursor)
    conn.close()

def main():
    load_dotenv()
    create_w3_contract()
    update_cex_fee()
    calc_daily()
    calc_apy()
    #upload_to_gdrive('daily_count.xlsx')

if __name__ == "__main__":
    main()