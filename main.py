from calc_daily import calc_daily
from tool import update_bundle, update_the_bundle
from calc_return import calc_return
from dotenv import load_dotenv
from base import update_base
from op import update_op

def main():
    load_dotenv()
    #update_bundle()
    update_base()
    #calc_daily()
    update_the_bundle('base', 19008104, 4856)
    calc_return("base")
   

if __name__ == "__main__":
    main()
