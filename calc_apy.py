import sqlite3
import time
from decimal import Decimal
from datetime import datetime
import pandas as pd
from tool import round_decimal, get_token_id

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
    },
        {
        "start_date": 1733184000,
        "ETH": 3.1,
        "USDC": 320000,
        "BTC": 2,
        "DAI": 0,
        "USDT": 100000
    }
]

def get_base_capital(date, coin):
    for capital in reversed(capitals):
        if date >= capital["start_date"]:
            return Decimal(capital[coin])
    return 0

def get_capital_with_previous_profit(coin, date, current_capital, last_day_profit):
    capital = get_base_capital(date, coin)
    last_day_capital = get_base_capital(date - 86400, coin)
    capital = Decimal(Decimal(current_capital) + capital - last_day_capital)

    return Decimal(capital + Decimal(last_day_profit))

# get the profit of the date
def get_profit(df, date, coin):
    profit = 0
    coin_id = get_token_id(coin)

    for table in df:
        for index, row in df[table].iterrows():
            # Convert the date to string and remove the decimal part
            date_str = str(int(row["Date"]))
            date_row = pd.to_datetime(date_str, format='%Y%m%d').date()
            if date_row == datetime.fromtimestamp(date).date():
                token_price_to_eth = get_token_price_in_df(df, date)[1] / get_token_price_in_df(df, date)[0]
                gas_to_coin = 0
                if row["Total Gas Fee(ETH)"] != 0:
                    gas_to_coin = Decimal(row["Total Gas Fee(ETH)"]) * token_price_to_eth
                
                profit += Decimal(row["Total Input Amount"] - row["Total Output Amount"] - row["Total LP Fee"]) - gas_to_coin
                break
    return Decimal(profit)

def get_apy_by_profit(profit, capital):
    if capital == 0:
        return "0.00%"
    apy = round_decimal(profit * 365 / capital , 4)
    return f"{apy * 100:.2f}%"

def get_token_price_in_df(df, date):
    for table in df:
        for index, row in df[table].iterrows():
            date_str = str(int(row["Date"]))
            date_row = pd.to_datetime(date_str, format='%Y%m%d').date()
            if date_row == datetime.fromtimestamp(date).date():
                return Decimal(row["Token Price"]), Decimal(row["ETH Price"])
    return 0, 0
def calc_apy_daily():
    print("Calculating APY")
    data = []
    
    # Load the existing data from the Excel sheet
    try:
        existing_df = pd.read_excel('daily_count.xlsx', sheet_name='APY')
    except FileNotFoundError:
        existing_df = pd.DataFrame(columns=["Date","ETH Capital", "ETH Profit", "ETH APY", "USDC Capital", "USDC Profit", "USDC APY", "BTC Capital", "BTC Profit", "BTC APY", "Dai Capital", "Dai Profit", "Dai APY", "USDT Capital", "USDT Profit", "USDT APY", "Total Profit", "Total APY"])
    last_btc_profit = 0
    last_eth_profit = 0
    last_usdc_profit = 0
    last_dai_profit = 0
    last_usdt_profit = 0

    # Determine the start time and initialize the last capital values based on the existing data
    if not existing_df.empty:
        last_date_str = existing_df.iloc[-1]['Date']
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
        start_time = last_date.timestamp()  # Start from the next day
        last_eth_capital = existing_df.iloc[-1]['ETH Capital']
        last_usdc_capital = existing_df.iloc[-1]['USDC Capital']
        last_btc_capital = existing_df.iloc[-1]['BTC Capital']
        last_dai_capital = existing_df.iloc[-1]['Dai Capital']
        last_usdt_capital = existing_df.iloc[-1]['USDT Capital']
        last_btc_profit = existing_df.iloc[-1]['BTC Profit']
        last_eth_profit = existing_df.iloc[-1]['ETH Profit']
        last_usdc_profit = existing_df.iloc[-1]['USDC Profit']
        last_dai_profit = existing_df.iloc[-1]['Dai Profit']
        last_usdt_profit = existing_df.iloc[-1]['USDT Profit']
    else:
        start_time = 1724803200
        last_eth_capital = 0
        last_usdc_capital = 0
        last_btc_capital = 0
        last_dai_capital = 0
        last_usdt_capital = 0

    # Load the Excel data to calculate the APY
    df_usdc = pd.read_excel('daily_count.xlsx', sheet_name=['base_usdc', 'eth_usdc', 'arb_usdc'])
    df_eth = pd.read_excel('daily_count.xlsx', sheet_name=['base_weth', 'op_weth', 'arb_weth', 'eth_weth'])
    df_btc = pd.read_excel('daily_count.xlsx', sheet_name=['eth_wbtc'])
    df_dai = pd.read_excel('daily_count.xlsx', sheet_name=['eth_dai'])
    df_usdt = pd.read_excel('daily_count.xlsx', sheet_name=['eth_usdt'])
    
    end_time = time.time()
    usdc_capital = last_usdc_capital
    eth_capital = last_eth_capital
    btc_capital = last_btc_capital
    dai_capital = last_dai_capital
    usdt_capital = last_usdt_capital

    while start_time < end_time:

        date_str = datetime.fromtimestamp(start_time).strftime("%d-%m-%Y")

        #calc usdc apy
        usdc_capital = get_capital_with_previous_profit("USDC", start_time, usdc_capital, last_usdc_profit)
        usdc_profit = get_profit(df_usdc, start_time, "USDC")
        usdc_apy = get_apy_by_profit(usdc_profit, usdc_capital)
        #calc eth apy
        eth_capital = get_capital_with_previous_profit("ETH", start_time, eth_capital, last_eth_profit)
        eth_profit = get_profit(df_eth, start_time, "ETH")
        eth_apy = get_apy_by_profit(eth_profit, eth_capital)
        
        #calc btc apy
        btc_capital = get_capital_with_previous_profit("BTC", start_time, btc_capital, last_btc_profit)
        btc_profit = get_profit(df_btc, start_time, "BTC")
        btc_apy = get_apy_by_profit(btc_profit, btc_capital)

        #calc dai apy
        dai_capital = get_capital_with_previous_profit("DAI", start_time, dai_capital, last_dai_profit)
        dai_profit = get_profit(df_dai, start_time, "DAI")
        dai_apy = get_apy_by_profit(dai_profit, dai_capital)

        #calc usdt apy
        usdt_capital = get_capital_with_previous_profit("USDT", start_time, usdt_capital, last_usdt_profit)
        usdt_profit = get_profit(df_usdt, start_time, "USDT")
        usdt_apy = get_apy_by_profit(usdt_profit, usdt_capital)
        
        weth_price = get_token_price_in_df(df_eth, start_time)[0];
        usdc_price = get_token_price_in_df(df_usdc, start_time)[0]

        wbtc_price = get_token_price_in_df(df_btc, start_time)[0]

        dai_price = get_token_price_in_df(df_dai, start_time)[0]

        usdt_price = get_token_price_in_df(df_usdt, start_time)[0]

        total_profit = eth_profit * weth_price + usdc_profit * usdc_price + btc_profit * wbtc_price + dai_profit * dai_price + usdt_profit * usdt_price
        
        total_capital = eth_capital * weth_price + usdc_capital * usdc_price + btc_capital * wbtc_price + dai_capital * dai_price + usdt_capital * usdt_price

        total_apy = get_apy_by_profit(total_profit, total_capital)
        date_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d")
        
        data.append(
            [date_str, eth_capital, eth_profit, eth_apy, usdc_capital, usdc_profit, usdc_apy, btc_capital, btc_profit, btc_apy, dai_capital, dai_profit, dai_apy, usdt_capital, usdt_profit, usdt_apy, total_capital, total_profit, total_apy]
        )
        start_time += 86400
        last_btc_profit = btc_profit
        last_eth_profit = eth_profit
        last_usdc_profit = usdc_profit
        last_dai_profit = dai_profit
        last_usdt_profit = usdt_profit


    # Convert the new data to a DataFrame
    new_df = pd.DataFrame(data, columns=["Date","ETH Capital", "ETH Profit", "ETH APY", "USDC Capital", "USDC Profit", "USDC APY", "BTC Capital", "BTC Profit", "BTC APY", "Dai Capital", "Dai Profit", "Dai APY", "USDT Capital", "USDT Profit", "USDT APY", "Total Capital", "Total Profit", "Total APY"])
    if existing_df.empty:
        combined_df = new_df
    elif new_df.empty:
        combined_df = existing_df
    else:
        # Append the new data to the existing data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Remove the duplicate rows
        combined_df = combined_df.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')
    # Save the combined data back to the Excel sheet
    with pd.ExcelWriter('daily_count.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        combined_df.to_excel(writer, sheet_name='APY', index=False)

def calc_apy():
    calc_apy_daily()

def main():
    calc_apy()

# Example usage
if __name__ == "__main__":
    main()