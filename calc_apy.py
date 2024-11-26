import sqlite3
import time
from decimal import Decimal
from datetime import datetime
import pandas as pd
from tool import get_token_price, round_decimal

# Hard-coded data
capitals = [
    {
        "start_date": 1724803200,
        "ETH": 3.1,
        "USDC": 30000,
        "BTC": 0,
        "DAI": 0,
        "USDT": 0
    },
    {
        "start_date": 1726099200,
        "ETH": 3.1,
        "USDC": 30000,
        "BTC": 2,
        "DAI": 0,
        "USDT": 0
    },
    {
        "start_date": 1727395200,
        "ETH": 43.1,
        "USDC": 30000,
        "BTC": 2,
        "DAI": 100000,
        "USDT": 0
    },
    {
        "start_date": 1728518400,
        "ETH": 3.1,
        "USDC": 30000,
        "BTC": 2,
        "DAI": 100000,
        "USDT": 0
    },
    {
        "start_date": 1730332800,
        "ETH": 3.1,
        "USDC": 230000,
        "BTC": 2,
        "DAI": 100000,
        "USDT": 100000
    }
]

def get_base_capital(date, coin):
    for capital in reversed(capitals):
        if date >= capital["start_date"]:
            return capital[coin]
    return 0

def get_capital_with_previous_profit(df, coin, date):
    capital = get_base_capital(date, coin)
    for table in df:
        for index, row in df[table].iterrows():
            date_row  = pd.to_datetime(str(int(row["Date"])), format='%Y%m%d').timestamp()
            if date_row >= date:
                break
            profit = row["Total Input Amount"] - row["Total Output Amount"] - row["Total LP Fee"] - row["Total LP Fee"]
            capital += profit 
    return Decimal(capital)
# get the profit of the date
def get_profit(df, date):
    profit = 0
    for table in df:
        for index, row in df[table].iterrows():
            date_row  = pd.to_datetime(str(int(row["Date"])), format='%Y%m%d').timestamp()
            if date_row == date:
                profit += row["Total Input Amount"] - row["Total Output Amount"] - row["Total LP Fee"] - row["Total Gas Fee"]
    return Decimal(profit)

def get_apy_by_profit(profit, capital):
    if capital == 0:
        return "0.00%"
    apy = round_decimal(profit / capital * 365, 4)
    return f"{apy * 100:.2f}%"

def calc_apy_daily(cursor):
    print("Calculating APY")
    data = []
    # calc usdc apy
    # load the excel data to calculate the APY
    df_usdc = pd.read_excel('daily_count.xlsx', sheet_name=['base_usdc', 'eth_usdc'])
    df_eth = pd.read_excel('daily_count.xlsx', sheet_name=['base_weth', 'op_weth', 'arb_weth', 'eth_weth'])
    df_btc = pd.read_excel('daily_count.xlsx', sheet_name=['eth_wbtc'])
    df_dai = pd.read_excel('daily_count.xlsx', sheet_name=['eth_dai'])
    df_usdt = pd.read_excel('daily_count.xlsx', sheet_name=['eth_usdt'])

    wbtc_price = Decimal(get_token_price("wrapped-bitcoin"))
    weth_price = Decimal(get_token_price("weth"))
    usdc_price = Decimal(get_token_price("usd-coin"))
    dai_price = Decimal(get_token_price("dai"))
    usdt_price = Decimal(get_token_price("tether"))
    
    end_time = time.time()
    start_time = 1724803200
    while start_time < end_time:
        date_str = datetime.fromtimestamp(start_time).strftime("%Y%m%d")
        #calc usdc apy
        usdc_capital = get_capital_with_previous_profit(df_usdc, "USDC", start_time)
        
        usdc_profit = get_profit(df_usdc, start_time)
        usdc_apy = get_apy_by_profit(usdc_profit, usdc_capital)
        print(f"USDC: {usdc_capital}, {usdc_profit}, {usdc_apy}")

        #calc eth apy
        eth_capital = get_capital_with_previous_profit(df_eth, "ETH", start_time)
        eth_profit = get_profit(df_eth, start_time)
        eth_apy = get_apy_by_profit(eth_profit, eth_capital)
        
        #calc btc apy
        btc_capital = get_capital_with_previous_profit(df_btc, "BTC", start_time)
        btc_profit = get_profit(df_btc, start_time)
        btc_apy = get_apy_by_profit(btc_profit, btc_capital)

        #calc dai apy
        dai_capital = get_capital_with_previous_profit(df_dai, "DAI", start_time)
        dai_profit = get_profit(df_dai, start_time)
        dai_apy = get_apy_by_profit(dai_profit, dai_capital)

        #calc usdt apy
        usdt_capital = get_capital_with_previous_profit(df_usdt, "USDT", start_time)
        usdt_profit = get_profit(df_usdt, start_time)
        usdt_apy = get_apy_by_profit(usdt_profit, usdt_capital)

        total_profit = eth_profit * weth_price + usdc_profit * usdc_price + btc_profit * wbtc_price + dai_profit * dai_price + usdt_profit * usdt_price
        total_capital = eth_capital * weth_price + usdc_capital * usdc_price + btc_capital * wbtc_price + dai_capital * dai_price + usdt_capital * usdt_price
        total_apy = get_apy_by_profit(total_profit, total_capital)
        
        data.append(
            [date_str, eth_capital, eth_profit, eth_apy, usdc_capital, usdc_profit, usdc_apy, btc_capital, btc_profit, btc_apy, dai_capital, dai_profit, dai_apy, usdt_capital, usdt_profit, usdt_apy, total_profit, total_apy]
        )
        start_time += 86400

    
    df = pd.DataFrame(data, columns=["Date","ETH Capital", "ETH Profit", "ETH APY", "USDC Capital", "USDC Profit", "USDC APY", "BTC Capital", "BTC Profit", "BTC APY", "Dai Capital", "Dai Profit", "Dai APY", "USDT Capital", "USDT Profit", "USDT APY", "Total Profit", "Total APY"])
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=f'APY', index=False)

def calc_apy():
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    calc_apy_daily(cursor)
    conn.close()

def main():
    calc_apy()

if __name__ == "__main__":
    main()