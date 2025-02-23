from util import format_message
from urllib import parse
from os import getenv
from dotenv import load_dotenv
import pandas as pd
from playwright.sync_api import BrowserContext, expect
load_dotenv()


def execute(browser: BrowserContext, args):
    with open('./data/message_local.txt', 'r', encoding='utf-8') as f:
        unparsed_message = f.read()

    WAIT_TIME = int(getenv('WAIT_TIME'))

    df = pd.read_excel('./data/phones_local.xlsx')
    df['PHONE'] = df['PHONE'].astype(str)
    df['STATUS'] = df['STATUS'].notna().astype(bool)    
    working_df = df[df['STATUS'] != True].head(args['COUNT'])


    page = browser.new_page()
    send_url = 'https://web.whatsapp.com/send?'

    for index, row in working_df.iterrows():
        message = format_message(row, unparsed_message)

        send_params = parse.urlencode(
            {'phone': row['PHONE'], 'text': message})

        print(row['PHONE'], send_url + send_params)

        page.goto(send_url + send_params)

        expect(page.locator(
            '//div[@id="side"]')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)

        page.wait_for_timeout(1000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        print(invalid_number)

        if (invalid_number is True):
            print(f'{row['PHONE']} is an invalid phone number')

            df.loc[index, 'STATUS'] = True

            page.wait_for_timeout(WAIT_TIME)
            continue

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

        df.loc[index, 'STATUS'] = True

        page.wait_for_timeout(WAIT_TIME)

    df.to_excel('./data/phones_local.xlsx', index=False)
