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
import json
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

    page.goto("https://web.whatsapp.com/")

    main_url = 'https://wa.me/'
    rows = df.iterrows()
    
    total = 0
    recorded = 0
    
    for _, row in rows:
        total += 1
        send_params = row['CUS_MOBILE_1']

        send_url = main_url + send_params

        expect(page.locator(
            '//div[@id="side"]')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)

        page.evaluate(f"""
                    var wa_link_auto = document.createElement('a');
                    var link = document.createTextNode("hiding");
                    wa_link_auto.appendChild(link);
                    wa_link_auto.href = "{send_url}";
                    document.head.appendChild(wa_link_auto);
                    wa_link_auto.click();
        """
                      )

        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        if (invalid_number is True):
            print(f'{row['CUS_MOBILE_1']} is an invalid phone number')
            page.wait_for_timeout(WAIT_TIME)
            continue

        X_CONTACT_NAME = '//*[@id="main"]/header/div[2]/div/div/div/div/span'
        expect(page.locator(X_CONTACT_NAME)).to_be_visible(timeout=50000)
        contact_name = page.locator(X_CONTACT_NAME)
        page.wait_for_timeout(10000)
        messages_container = page.locator('//*[@id="main"]/div[2]/div')
        messages_container.focus()

        print(f'Getting NPS Score for {contact_name.text_content()}')

        messages = get_whatsapp_messages(page, '')
        print(messages)

        
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
        status = False
        for i in reversed(range(len(messages))):
            message = messages[i]
            
            if (message['sender'] == 'out'):
                break

            matches = re.findall(
                r'([0-9]+|[\u0660-\u0669]+)', message['content'])

            if matches:
                nps = min([int(match) for match in matches])
                if(nps < 0):
                    print(f'recieved number is not a valid NPS, {nps}')
                    continue
                
                if(nps > 10):
                    nps = 10
                    
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
                status =True
                recorded += 1
                break
        if not status:
            data = {
                'RESPONDED': 0,
                'RESPONSE': None,
                'NPS': None,
                'MONTH': MONTH,
                'YEAR': YEAR,
                'CUS_MOBILE_1': row['CUS_MOBILE_1']
            }
            update_nps(data, cursor, conn)
        print("status: ", status)
        print(f'NPS RECORD {recorded}/{total}')
        page.wait_for_timeout(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
