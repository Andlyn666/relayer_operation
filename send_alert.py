import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import sqlite3

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
# Example usage
if __name__ == "__main__":
    load_dotenv()
    send_slack_message("Hello, Slack!")