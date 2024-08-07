import mysql.connector.cursor
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from urllib import parse
from playwright.sync_api import sync_playwright, expect
from util import get_whatsapp_messages, get_message
user_data_location = r'C:\Users\Abdulrhman Jalal\AppData\Local\BraveSoftware\Brave-Browser\User Data\Default'
load_dotenv()

WAIT_TIME = 5000
COUNT = 1
MONTH = 6
YEAR = 2024

with open('./data/message.txt', 'r', encoding='utf-8') as f:
    unparsed_message = f.read()


def update_customer(data, cursor: mysql.connector.connect, conn: mysql.connector.connect):
    update_query = """
                    UPDATE phonedb.customer_phones 
                    SET WHATSAPP_EXISTS = %s, SEND_DATE = %s
                    WHERE (MONTH = %s AND YEAR = %s AND CUS_MOBILE_1 = %s)
                """
    cursor.execute(update_query, (data['WHATASAPP_EXISTS'],
                                  data['SEND_DATE'], MONTH, YEAR, data['CUS_MOBILE_1']))
    conn.commit()


with sync_playwright() as p:
    engine = create_engine(getenv('PHONE_CON'))
    query = f"""
                SELECT *
                FROM phonedb.customer_phones

                WHERE (MONTH = {MONTH} AND YEAR = {YEAR} AND SEND_DATE IS NULL)
                ORDER BY INV_TIME LIMIT {COUNT}
            """
    df = pd.read_sql(query, engine)

    conn = mysql.connector.connect(
        user=getenv('USER'),
        password=getenv('PASSWORD'),
        host=getenv('HOST')
    )

    cursor = conn.cursor()

    browser = p.chromium.launch_persistent_context(
        user_data_location, headless=False)

    page = browser.new_page()
    send_url = 'https://web.whatsapp.com/send?'

    for index, row in df.iterrows():
        message = get_message(row, unparsed_message)

        send_params = parse.urlencode(
            {'phone': row['CUS_MOBILE_1'], 'text': message})
        page.goto(send_url + send_params)

        expect(page.locator(
            '//*[@id="app"]/div/div[2]/div[3]/header/div[1]/div/img')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        if (invalid_number is True):
            print(f'{row['CUS_MOBILE_1']} is an invalid phone number')
            data = {
                'WHATASAPP_EXISTS': 0,
                'SEND_DATE': datetime.now(),
                'CUS_MOBILE_1': row['CUS_MOBILE_1']
            }
            update_customer(data, cursor, conn)

        X_CONTACT_NAME = '//*[@id="main"]/header/div[2]/div/div/div/span'
        expect(page.locator(X_CONTACT_NAME)).to_be_visible(timeout=50000)
        contact_name = page.locator(X_CONTACT_NAME)
        page.wait_for_timeout(1000)
        messages_container = page.locator('//div[@role="application"]')
        messages_container.focus()

        print(f'Sending message to {contact_name.text_content()}')

        send_button = page.locator('//button[@aria-label="Send"]')
        send_button.click()

        print(f'Sent message to {contact_name.text_content()}')
        print(f'Waiting...')

        data = {
            'WHATASAPP_EXISTS': 1,
            'SEND_DATE': datetime.now(),
            'CUS_MOBILE_1': row['CUS_MOBILE_1']
        }
        update_customer(data, cursor, conn)
        page.wait_for_timeout(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
