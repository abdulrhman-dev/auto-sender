import requests
import base64
from os import getenv
from dotenv import load_dotenv


load_dotenv()


def send_message(contact: str, message: str):
    url = f"{getenv('WAHA_BASE')}/api/sendText"

    headers = {
        "X-Api-Key": getenv("WAHA_API_KEY"),
        "Content-Type": "application/json",
        "accept": "application/json"
    }

    data = {
        "chatId": f"{contact}@c.us",
        "text": message,
        "session": "default"
    }

    return requests.post(url, json=data, headers=headers)


def get_messages(contact: str, limit: int = 10):
    url = f"{getenv('WAHA_BASE')}/api/default/chats/{contact}%40c.us/messages?downloadMedia=false&limit={limit}"
    headers = {
        "X-Api-Key": getenv("WAHA_API_KEY"),
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    return requests.get(url, headers=headers)
