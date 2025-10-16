from util import format_message, update_send_status
from urllib import parse
from datetime import datetime
from os import getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine
import mysql.connector
import mysql.connector.cursor
import pandas as pd
from playwright.sync_api import BrowserContext, expect
load_dotenv()


def execute(browser: BrowserContext, args):
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

    page.goto("https://web.whatsapp.com/")

    main_url = 'https://wa.me/'

    for _, row in df.iterrows():
        message = format_message(row, unparsed_message)

        send_params = row['CUS_MOBILE_1'] + '?' + parse.urlencode(
            {'text': message})

        send_url = main_url + send_params

        expect(page.locator(
            '//div[@id="side"]')).to_be_visible(timeout=100000)

        page.evaluate(f"""
                    var wa_link_auto = document.createElement('a');
                    var link = document.createTextNode("hiding");
                    wa_link_auto.appendChild(link);
                    wa_link_auto.href = "{send_url}";
                    document.head.appendChild(wa_link_auto);
                    wa_link_auto.click();
        """
                      )
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)
        page.wait_for_timeout(1750)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)
        # print(invalid_number)
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
            page.wait_for_timeout(WAIT_TIME)
            continue

        X_CONTACT_NAME = '//*[@id="main"]/header/div[2]/div/div/div/div/span'
        expect(page.locator(X_CONTACT_NAME)).to_be_visible(timeout=50000)
        contact_name = page.locator(X_CONTACT_NAME)
        page.wait_for_timeout(1000)
        messages_container = page.locator('//*[@id="main"]/div[2]/div')
        messages_container.focus()

        print(f'Sending message to {contact_name.text_content()}')

        send_button = page.locator('//div[@aria-label="Send"]')
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
