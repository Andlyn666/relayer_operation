from web3 import Web3
import sqlite3
from decimal import Decimal
from tool import get_bundle_id, get_relayer_root,get_chain_id
import pandas as pd
import os


def calc_bundle(cursor, start_block, end_block, bundle_id, chain, token, data, token_address):
    # get the sum of the output amount of Fill from start_block to end_block
    repay_chain_id = get_chain_id(chain)
    cursor.execute(
        """
        SELECT input_amount, tx_hash, lp_fee FROM Fill WHERE block >= ? AND block <= ? AND repayment_chain = ? AND is_success = 1 AND (output_token = ? OR (input_token = ? AND repayment_chain = origin_chain)) 
        """,
        (
            start_block,
            end_block,
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
            "start_block": start_block,
            "end_block": end_block,
            "tx_hashs": tx_hashs,
            "relayer_root": relayer_root,
        }
    )
    return data

def calc_return(chain):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    repayment_chain_id = get_chain_id(chain)
    fill_list = cursor.execute(
        """
        SELECT * FROM Fill WHERE repayment_chain = ? AND is_return is NULL AND is_success = 1 ORDER BY block ASC
        """,
        (repayment_chain_id,),
    ).fetchall()
    print(f"Calculating return for {chain}")
    data_usdc = []
    data_weth = []
    data_wbtc = []
    data_dai = []
    current_bundle_id = get_bundle_id(fill_list[0][13], cursor, chain)
    start_block = fill_list[0][13]
    end_block = 0
    usdc_address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    weth_address = "0x4200000000000000000000000000000000000006"
    dai_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    wbtc_address = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
    if chain == "arb":
        weth_address = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        dai_address = "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1"
    if chain == "eth":
        weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    if chain == "base":
        dai_address = "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb"
    for fill in fill_list:
        bundle_id = get_bundle_id(fill[13], cursor, chain)
        if bundle_id == 0:
            continue  # Skip this iteration instead of breaking
        if bundle_id != current_bundle_id:
            end_block = int(fill[13]) - 1
            data_usdc = calc_bundle(
                cursor,
                start_block,
                end_block,
                current_bundle_id,
                chain,
                "usdc",
                data_usdc,
                usdc_address
            )
            data_weth = calc_bundle(
                cursor,
                start_block,
                end_block,
                current_bundle_id,
                chain,
                "weth",
                data_weth,
                weth_address,
            )
            data_wbtc = calc_bundle(
                cursor,
                start_block,
                end_block,
                current_bundle_id,
                chain,
                "wbtc",
                data_wbtc,
                wbtc_address,
            )
            data_dai = calc_bundle(
                cursor,
                start_block,
                end_block,
                current_bundle_id,
                chain,
                "dai",
                data_dai,
                dai_address,
            )
            current_bundle_id = bundle_id
            start_block = int(fill[13])
    conn.close()
    # Convert data to DataFrame and write to Excel
    df_usdc = pd.DataFrame(data_usdc)
    # Convert data to DataFrame and write to Excel
    df_weth = pd.DataFrame(data_weth)
    df_wbtc = pd.DataFrame(data_wbtc)
    df_dai = pd.DataFrame(data_dai)
    # Define the file name
    file_name = "return_data.xlsx"

    with pd.ExcelWriter(file_name, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        if len(data_usdc) > 0:
            df_usdc.to_excel(writer, sheet_name=f"{chain}-USDC", index=False)
        if len(data_weth) > 0:
            df_weth.to_excel(writer, sheet_name=f"{chain}-WETH", index=False)
        if len(data_wbtc) > 0:
            df_wbtc.to_excel(writer, sheet_name=f"{chain}-WBTC", index=False)
        if len(data_dai) > 0:
            df_dai.to_excel(writer, sheet_name=f"{chain}-DAI", index=False)
    return