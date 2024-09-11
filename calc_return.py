from web3 import Web3
import sqlite3
from decimal import Decimal
from tool import get_bundle_id

def calc_bundle(cursor, start_block, end_block, bundle_id, chain, token):
    if token == "usdc":
        # get the sum of the output amount of Fill from start_block to end_block
        cursor.execute(
            """
            SELECT input_amount FROM Fill WHERE block >= ? AND block <= ? AND aim_chain = ? AND is_success = 1 AND output_token = ?
            """,
            (start_block, end_block, chain, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        print(f"bundle {bundle_id}{chain} {token} input_amount: {total_output_amount}")
        # get the Return value by bundle_id
        cursor.execute(
            """
            SELECT output_amount FROM Return WHERE bundle_id = ? AND aim_chain = ? AND output_token = ?
            """,
            (int(bundle_id) + 1, chain, '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'),
        )
        return_amounts = cursor.fetchall()
        total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
        print(f"{chain} {token} return_amount: {total_return_amount}")
    if token == "weth":
        # get the sum of the output amount of Fill from start_block to end_block
        cursor.execute(
            """
            SELECT input_amount FROM Fill WHERE block >= ? AND block <= ? AND aim_chain = ? AND is_success = 1 AND output_token = ?
            """,
            (start_block, end_block, chain, '0x4200000000000000000000000000000000000006'),
        )
        fill_amounts = cursor.fetchall()
        total_output_amount = sum(Decimal(amount[0]) for amount in fill_amounts)
        print(f"{chain} {token} input_amount: {total_output_amount}")
        # get the Return value by bundle_id
        cursor.execute(
            """
            SELECT output_amount FROM Return WHERE bundle_id = ? AND aim_chain = ? AND output_token = ?
            """,
            (int(bundle_id) + 1, chain, '0x4200000000000000000000000000000000000006'),
        )
        return_amounts = cursor.fetchall()
        total_return_amount = sum(Decimal(amount[0]) for amount in return_amounts)
        print(f"{chain} {token} return_amount: {total_return_amount}")
def calc_return(chain):
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    fill_list = cursor.execute(
        """
        SELECT * FROM Fill WHERE aim_chain = ? AND is_return is NULL ORDER BY block ASC
        """,
        (chain,),
    ).fetchall()

    current_bundle_id = get_bundle_id(fill_list[0][12], cursor)
    start_block = fill_list[0][12]
    end_block = 0
    for fill in fill_list:
           bundle_id = get_bundle_id(fill[12], cursor)
           if bundle_id != current_bundle_id:
               end_block = int(fill[12]) - 1
               calc_bundle(cursor, start_block, end_block, current_bundle_id, chain, 'usdc')
               calc_bundle(cursor, start_block, end_block, current_bundle_id, chain, 'weth')
               current_bundle_id = bundle_id
               start_block = fill[12]
    conn.close()
    return