from calc_daily import calc_daily
from tool import update_the_bundle, create_w3_contract, update_deposit_time
from calc_return import calc_return
from dotenv import load_dotenv
from base import update_base
from op import update_op
from arb import update_arb
from eth import update_eth
from web3 import Web3
import os

def main():
    load_dotenv()
    create_w3_contract()
    update_deposit_time()
    #update_the_bundle('base', 19008104, 4857)
    # update_base()
    
    # #calc_return("base")

    # update_op()
    # #update_the_bundle("op", 123939334, 5784)
    # #calc_return("op")

    # update_arb()
    # #update_the_bundle("arb", 247363892, 6080)
    # #calc_return("arb")
    
    # update_eth()
    #update_the_bundle('eth', 19008104, 4857)
    #calc_return("eth")

    calc_daily()


if __name__ == "__main__":
    main()
