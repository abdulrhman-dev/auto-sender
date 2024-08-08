from playwright.sync_api import BrowserContext, expect
import pandas as pd
import re
import mysql.connector.cursor
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from urllib import parse
from util import format_message, update_send_status, update_nps, get_whatsapp_messages
load_dotenv()


def send_messages(browser: BrowserContext, args):
    with open('./data/message.txt', 'r', encoding='utf-8') as f:
        unparsed_message = f.read()

    MONTH = args['MONTH']
    YEAR = args['YEAR']
    COUNT = args['COUNT']
    WAIT_TIME = int(getenv('WAIT_TIME'))

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

    page = browser.new_page()
    send_url = 'https://web.whatsapp.com/send?'

    for _, row in df.iterrows():
        message = format_message(row, unparsed_message)

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
                'CUS_MOBILE_1': row['CUS_MOBILE_1'],
                'YEAR': YEAR,
                'MONTH': MONTH
            }
            update_send_status(data, cursor, conn)

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
            'CUS_MOBILE_1': row['CUS_MOBILE_1'],
            'YEAR': YEAR,
            'MONTH': MONTH
        }
        update_send_status(data, cursor, conn)
        page.wait_for_timeout(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()


def store_nps(browser: BrowserContext, args):
    MONTH = args['MONTH']
    YEAR = args['YEAR']
    COUNT = args['COUNT']
    WAIT_TIME = int(getenv('WAIT_TIME'))

    engine = create_engine(getenv('PHONE_CON'))
    query = f"""
        SELECT *
        FROM phonedb.customer_phones
        WHERE (MONTH = {MONTH} AND YEAR = {YEAR} AND WHATSAPP_EXISTS = 1 AND RESPONDED IS NULL)
        ORDER BY INV_TIME LIMIT {COUNT}
    """

    df = pd.read_sql(query, engine)

    conn = mysql.connector.connect(
        user=getenv('USER'),
        password=getenv('PASSWORD'),
        host=getenv('HOST')
    )

    cursor = conn.cursor()

    page = browser.new_page()
    send_url = 'https://web.whatsapp.com/send?'

    for _, row in df.iterrows():
        send_params = parse.urlencode(
            {'phone': row['CUS_MOBILE_1']})
        page.goto(send_url + send_params)

        expect(page.locator(
            '//*[@id="app"]/div/div[2]/div[3]/header/div[1]/div/img')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        if (invalid_number is True):
            print(f'{row['CUS_MOBILE_1']} is an invalid phone number')

        X_CONTACT_NAME = '//*[@id="main"]/header/div[2]/div/div/div/span'
        expect(page.locator(X_CONTACT_NAME)).to_be_visible(timeout=50000)
        contact_name = page.locator(X_CONTACT_NAME)
        page.wait_for_timeout(1000)
        messages_container = page.locator('//div[@role="application"]')
        messages_container.focus()

        print(f'Getting NPS Score for {contact_name.text_content()}')

        messages = get_whatsapp_messages(page, 'in')

        if len(messages) == 0:
            data = {
                'RESPONDED': 0,
                'RESPONSE': None,
                'NPS': None,
                'MONTH': MONTH,
                'YEAR': YEAR,
                'CUS_MOBILE_1': row['CUS_MOBILE_1']
            }
            update_nps(data, cursor, conn)

        for i in reversed(range(len(messages))):
            message = messages[i]

            matches = re.findall(r'\d+', message['content'])

            if matches:
                nps = min([int(match) for match in matches])
                print(f'Got NPS Score for {contact_name.text_content()}')
                print(f'NPS = {nps}')
                data = {
                    'RESPONDED': 1,
                    'RESPONSE': message['content'],
                    'NPS': nps,
                    'MONTH': MONTH,
                    'YEAR': YEAR,
                    'CUS_MOBILE_1': row['CUS_MOBILE_1']
                }
                update_nps(data, cursor, conn)
                break

            if i == 0:
                data = {
                    'RESPONDED': 0,
                    'RESPONSE': messages[0]['content'],
                    'NPS': None,
                    'MONTH': MONTH,
                    'YEAR': YEAR,
                    'CUS_MOBILE_1': row['CUS_MOBILE_1']
                }
                update_nps(data, cursor, conn)
        page.wait_for_timeout(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
