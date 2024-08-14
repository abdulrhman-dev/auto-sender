from util import update_nps, get_whatsapp_messages
from urllib import parse
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine
import mysql.connector
import mysql.connector.cursor
import re
import pandas as pd
from playwright.sync_api import BrowserContext, expect
import sys
sys.path.append('../')


load_dotenv()


def execute(browser: BrowserContext, args):
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
            '//div[@id="side"]')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        if (invalid_number is True):
            print(f'{row['CUS_MOBILE_1']} is an invalid phone number')
            page.wait_for_timeout(WAIT_TIME)
            continue

        X_CONTACT_NAME = '//*[@id="main"]/header/div[2]/div/div/div/span'
        expect(page.locator(X_CONTACT_NAME)).to_be_visible(timeout=50000)
        contact_name = page.locator(X_CONTACT_NAME)
        page.wait_for_timeout(1000)
        messages_container = page.locator('//div[@role="application"]')
        messages_container.focus()

        print(f'Getting NPS Score for {contact_name.text_content()}')

        messages = get_whatsapp_messages(page, '')
        if (len(messages) == 0):
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

            if (message['sender'] == 'out'):
                data = {
                    'RESPONDED': 0,
                    'RESPONSE': None,
                    'NPS': None,
                    'MONTH': MONTH,
                    'YEAR': YEAR,
                    'CUS_MOBILE_1': row['CUS_MOBILE_1']
                }
                update_nps(data, cursor, conn)
                break

            matches = re.findall(
                r'([0-9]+|[\u0660-\u0669]+)', message['content'])

            if matches:
                nps = min([int(match) for match in matches])
                if (nps > 10 or nps < 0):
                    print(f'recieved number is not a valid NPS, {nps}')
                    continue
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

        page.wait_for_timeout(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
