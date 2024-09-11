from web3 import Web3
import sqlite3
from decimal import Decimal
from tool import get_bundle_id, get_relayer_root
import pandas as pd

def calc_bundle(cursor, start_block, end_block, bundle_id, chain, token, data):
    if token == "usdc":
        # get the sum of the output amount of Fill from start_block to end_block
        cursor.execute(
            """
            SELECT input_amount, tx_hash FROM Fill WHERE block >= ? AND block <= ? AND aim_chain = ? AND is_success = 1 AND output_token = ?
            """,
            (start_block, end_block, chain, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        # joint the tx hash with , to show in the excel
        tx_hashs = ", ".join([amount[1] for amount in fill_amounts])

        # get the Return value by bundle_id
        cursor.execute(
            """
            SELECT output_amount, tx_hash FROM Return WHERE bundle_id = ? AND aim_chain = ? AND output_token = ?
            """,
            (int(bundle_id), chain, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'),
        )
        return_amounts = cursor.fetchall()
        if len(return_amounts) == 0:
            return data
        total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
        relayer_root = get_relayer_root(chain, cursor, bundle_id)
        data.append({
            "bundle_id": bundle_id,
            "chain": chain,
            "token": token,
            "tx_hash": return_amounts[0][1],
            "input_amount": total_output_amount,
            "return_amount": total_return_amount,
            "start_block": start_block,
            "end_block": end_block,
            "tx_hashs": tx_hashs,
            "relayer_root": relayer_root
        })

    if token == "weth":
        # get the sum of the output amount of Fill from start_block to end_block
        cursor.execute(
            """
            SELECT input_amount, tx_hash FROM Fill WHERE block >= ? AND block <= ? AND aim_chain = ? AND is_success = 1 AND output_token = ?
            """,
            (start_block, end_block, chain, '0x4200000000000000000000000000000000000006'),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        # joint the tx hash with , to show in the excel
        tx_hashs = ", ".join([amount[1] for amount in fill_amounts])
        # get the Return value by bundle_id
        cursor.execute(
            """
            SELECT output_amount, tx_hash FROM Return WHERE bundle_id = ? AND aim_chain = ? AND output_token = ?
            """,
            (int(bundle_id), chain, '0x4200000000000000000000000000000000000006'),
        )
        return_amounts = cursor.fetchall()
        if len(return_amounts) == 0:
            return data
        relayer_root = get_relayer_root(chain, cursor, bundle_id)
        total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
        data.append({
            "bundle_id": bundle_id,
            "chain": chain,
            "token": token,
            "tx_hash": return_amounts[0][1],
            "input_amount": total_output_amount,
            "return_amount": total_return_amount,
            "start_block": start_block,
            "end_block": end_block,
            "tx_hashs": tx_hashs,
            "relayer_root": relayer_root
        })
    return data

def calc_return(chain):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    fill_list = cursor.execute(
        """
        SELECT * FROM Fill WHERE aim_chain = ? AND is_return is NULL ORDER BY block ASC
        """,
        (chain,),
    ).fetchall()
    data_usdc = []
    data_weth = []
    current_bundle_id = get_bundle_id(fill_list[0][12], cursor)
    start_block = fill_list[0][12]
    end_block = 0
    for fill in fill_list:
           bundle_id = get_bundle_id(fill[12], cursor)
           if bundle_id == 0:
               break
           if bundle_id != current_bundle_id:
               end_block = int(fill[12]) - 1
               data_usdc = calc_bundle(cursor, start_block, end_block, current_bundle_id, chain, 'usdc', data_usdc)
               data_weth = calc_bundle(cursor, start_block, end_block, current_bundle_id, chain, 'weth', data_weth)
               current_bundle_id = bundle_id
               start_block = fill[12]
    conn.close()
    # Convert data to DataFrame and write to Excel
    df_usdc = pd.DataFrame(data_usdc)
    # Convert data to DataFrame and write to Excel
    df_weth = pd.DataFrame(data_weth)
    with pd.ExcelWriter(f'{chain}_data.xlsx') as writer:
        df_usdc.to_excel(writer, sheet_name='USDC', index=False)
        df_weth.to_excel(writer, sheet_name='WETH', index=False)
    return
