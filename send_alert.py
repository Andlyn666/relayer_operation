import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import sqlite3
from web3 import Web3
import datetime

def send_slack_message(message, channel="#relayer-operation"):
    slack_token = os.getenv("SLACK_BOT_TOKEN")
    client = WebClient(token=slack_token)

    try:
        response = client.chat_postMessage(
            channel=channel,
            text=message
        )
        print(f"Message sent to {channel}: {response['message']['text']}")
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

def check_and_send_alert(bundle_id, chain, token, return_amount, input_amount):
    # check if this bundle is already send and recorded in the Alert table
    conn = sqlite3.connect("mydatabase.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM Alert WHERE bundle_id = ? AND chain = ? AND token = ?
        """,
        (bundle_id, chain, token),
    )
    alert = cursor.fetchall()
    if len(alert) == 0:
        cursor.execute(
            """
            INSERT INTO Alert (bundle_id, chain, token) VALUES (?, ?, ?)
            """,
            (bundle_id, chain, token),
        )
        conn.commit()
        message = f"Bundle {bundle_id} on {chain} {token} has a return amount of {return_amount} and input amount of {input_amount}"
        send_slack_message(message)
    print(f"Alert sent for bundle {bundle_id} on {chain} {token} with return amount {return_amount} and input amount {input_amount}")
    conn.close()

def check_eth_balance_and_send_alert():
    relayer_address = '0x84A36d2C3d2078c560Ff7b62815138a16671b549'
    try:
        eth_rpc = os.getenv("ETH_RPC")
        web3_eth = Web3(Web3.HTTPProvider(eth_rpc))
        # Get the balance of the account
        balance = web3_eth.eth.get_balance(relayer_address)

        # Convert the balance from Wei to Ether
        balance_ether = web3_eth.from_wei(balance, 'ether')
        if balance_ether < 0.1:
            print(f"Balance of {relayer_address}: {balance_ether} ETH")
            send_slack_message(f"Relayer balance on ETH: {balance_ether} ETH")
    except Exception as e:
        print(f"Error checking balance: {e}")
    try:
        base_rpc = os.getenv("BASE_RPC")
        web3_base = Web3(Web3.HTTPProvider(base_rpc))
        balance = web3_base.eth.get_balance(relayer_address)
        balance_ether = web3_base.from_wei(balance, 'ether')
        if balance_ether < 0.01:
            print(f"Balance of {relayer_address}: {balance_ether} ETH")
            send_slack_message(f"Relayer balance on Base: {balance_ether} ETH")
    except Exception as e:
        print(f"Error checking balance: {e}")
    print ("Balance check completed at time: ", datetime.datetime.now())

# Example usage
if __name__ == "__main__":
    load_dotenv()
    check_eth_balance_and_send_alert()