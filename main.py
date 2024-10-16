from calc_daily import calc_daily
from tool import update_bundle, create_w3_contract, update_deposit_time, update_cex_fee
from calc_return import calc_return
from dotenv import load_dotenv
from base import update_base
from op import update_op
from arb import update_arb
from eth import update_eth
from web3 import Web3
import os
from upload_file import upload_to_gdrive

def main():
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

    update_cex_fee()
    
    calc_return("base")
    calc_return("op")
    calc_return("arb")
    calc_return("eth")

    calc_daily()
    upload_to_gdrive('daily_count.xlsx')
    upload_to_gdrive('return_data.xlsx')


if __name__ == "__main__":
    main()
