from decimal import Decimal
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from upload_file import upload_to_gdrive
from tool import create_w3_contract
from base import update_base
from op import update_op
from arb import update_arb
from eth import update_eth
from tool import update_bundle, get_bundle_id, update_deposit_time, get_relayer_root,get_chain_id, update_variable, get_variable
from send_alert import check_and_send_alert




def calc_bundle(cursor, start_time, end_time, bundle_id, chain, token, data, token_address):
    # get the sum of the output amount of Fill from start_block to end_block
    repay_chain_id = get_chain_id(chain)
    cursor.execute(
        """
        SELECT input_amount, tx_hash, lp_fee FROM Fill WHERE time_stamp >= ? AND time_stamp <= ? AND repayment_chain = ? AND is_success = 1 AND ((output_token = ? AND repayment_chain != origin_chain ) OR (input_token = ? AND repayment_chain = origin_chain)) 
        """,
        (
            start_time,
            end_time,
            repay_chain_id,
            token_address,
            token_address
        ),
    )
    fill_amounts = cursor.fetchall()
    total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
    total_lp_fee = sum(Decimal(amount[2] or 0) for amount in fill_amounts)
    # joint the tx hash with , to show in the excel
    tx_hashs = ", ".join([amount[1] for amount in fill_amounts])

    # get the Return value by bundle_id
    cursor.execute(
        """
        SELECT output_amount, tx_hash FROM Return WHERE bundle_id = ? AND aim_chain = ? AND output_token = ?
        """,
        (int(bundle_id), chain, token_address),
    )
    return_amounts = cursor.fetchall()
    total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
    relayer_root = get_relayer_root(chain, cursor, bundle_id)
    if len(return_amounts) == 0 and len(fill_amounts) == 0:
        return data
    data.append(
        {
            "bundle_id": bundle_id,
            "chain": chain,
            "token": token,
            "tx_hash": total_return_amount > 0 and return_amounts[0][1] or "",
            "input_amount": total_output_amount,
            "return_amount": total_return_amount,
            "lp_fee": total_lp_fee,
            "return + lp": total_return_amount + total_lp_fee,
            "start_time": start_time,
            "end_time": end_time,
            "tx_hashs": tx_hashs,
            "relayer_root": relayer_root,
        }
    )
    # if (total_output_amount - total_return_amount - total_lp_fee) / total_output_amount > 0.01, send 
    # calert to slack
    if (total_output_amount == 0):
        check_and_send_alert(bundle_id, chain, token, total_return_amount, total_output_amount)
        return data
    diff_percentage = (total_output_amount - total_return_amount - total_lp_fee) / total_output_amount
    if diff_percentage > 0.01:
        check_and_send_alert(bundle_id, chain, token, total_return_amount, total_output_amount)
    return data

def calc_return_by_chain(chain):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    repayment_chain_id = get_chain_id(chain)
    start_time = get_variable(f"last_return_time_{chain}")
    fill_list = cursor.execute(
        """
        SELECT * FROM Fill 
        WHERE repayment_chain = ? AND is_return is NULL AND is_success = 1 AND CAST(time_stamp AS INTEGER) > ?
        ORDER BY CAST(time_stamp AS INTEGER) ASC
        """,
        (repayment_chain_id, start_time, ),
    ).fetchall()
    print(f"Calculating return for {chain}")
    data_usdc = []
    data_weth = []
    data_wbtc = []
    data_dai = []
    data_usdt = []
    aim_chian = fill_list[0][9]
    current_bundle_id = get_bundle_id(fill_list[0][13], cursor, aim_chian, fill_list[0][17])
    start_time = fill_list[0][12]
    end_time = 0
    usdc_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    weth_address = "0x4200000000000000000000000000000000000006"
    dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    wbtc_address = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
    usdt_address = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
    if chain == "arb":
        weth_address = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        dai_address = "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1"
        usdc_address = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    if chain == "eth":
        weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    if chain == "base":
        dai_address = "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb"
    if chain == "op":
        usdc_address = "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"
    for fill in fill_list:
        aim_chian = fill[9]
        bundle_id = get_bundle_id(fill[13], cursor, aim_chian, fill[17])
        if bundle_id == 0:
            continue
        if bundle_id != current_bundle_id:
            end_time = int(fill[12]) - 1
            data_usdc = calc_bundle(
                cursor,
                start_time,
                end_time,
                current_bundle_id,
                chain,
                "usdc",
                data_usdc,
                usdc_address
            )
            data_weth = calc_bundle(
                cursor,
                start_time,
                end_time,
                current_bundle_id,
                chain,
                "weth",
                data_weth,
                weth_address,
            )
            data_wbtc = calc_bundle(
                cursor,
                start_time,
                end_time,
                current_bundle_id,
                chain,
                "wbtc",
                data_wbtc,
                wbtc_address,
            )
            data_dai = calc_bundle(
                cursor,
                start_time,
                end_time,
                current_bundle_id,
                chain,
                "dai",
                data_dai,
                dai_address,
            )
            data_usdt = calc_bundle(
                cursor,
                start_time,
                end_time,
                current_bundle_id,
                chain,
                "usdt",
                data_usdt,
                usdt_address,
            )
            current_bundle_id = bundle_id
            start_time = int(fill[12])
    
    conn.close()
    # Convert data to DataFrame and write to Excel
    df_usdc = pd.DataFrame(data_usdc)
    # Convert data to DataFrame and write to Excel
    df_weth = pd.DataFrame(data_weth)
    df_wbtc = pd.DataFrame(data_wbtc)
    df_dai = pd.DataFrame(data_dai)
    df_usdt = pd.DataFrame(data_usdt)
    # Define the file name
    file_name = "return_data.xlsx"

    with pd.ExcelWriter(file_name, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        def merge_and_write(sheet_name, new_data):
            try:
                existing_df = pd.read_excel(file_name, sheet_name=sheet_name)
                combined_df = pd.concat([existing_df, new_data]).drop_duplicates(subset=['bundle_id'], keep='last').sort_values('bundle_id')
            except (FileNotFoundError, ValueError):
                combined_df = new_data
            combined_df.to_excel(writer, sheet_name=sheet_name, index=False)

        if len(data_usdc) > 0:
            merge_and_write(f"{chain}-USDC", df_usdc)
        if len(data_weth) > 0:
            merge_and_write(f"{chain}-WETH", df_weth)
        if len(data_wbtc) > 0:
            merge_and_write(f"{chain}-WBTC", df_wbtc)
        if len(data_dai) > 0:
            merge_and_write(f"{chain}-DAI", df_dai)
        if len(data_usdt) > 0:
            merge_and_write(f"{chain}-USDT", df_usdt)
        
        update_variable(f"last_return_time_{chain}", end_time)
    return

def calc_return():
    load_dotenv()
    create_w3_contract()
    update_base()
    update_op()
    update_arb()
    update_eth()
    
    update_deposit_time()
    update_bundle('base', 19008104)
    update_bundle("op", 123939334)
    update_bundle("arb", 247363892)
    update_bundle('eth', 19008104)
    calc_return_by_chain("base")
    calc_return_by_chain("op")
    calc_return_by_chain("arb")
    calc_return_by_chain("eth")
    return

def main():
    calc_return()
    upload_to_gdrive('return_data.xlsx')

if __name__ == "__main__":
    main()