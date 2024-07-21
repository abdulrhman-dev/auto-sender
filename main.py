import pandas as pd
from datetime import datetime
from urllib import parse
from playwright.sync_api import sync_playwright, expect
from util import get_whatsapp_messages, get_message
user_data_location = r'C:\Users\Abdulrhman Jalal\AppData\Local\BraveSoftware\Brave-Browser\User Data\Default'


WAIT_TIME = 5000

with open('./data/message.txt', 'r', encoding='utf-8') as f:
    unparsed_message = f.read()


with sync_playwright() as p:
    browser = p.chromium.launch_persistent_context(
        user_data_location, headless=False)

    page = browser.new_page()
    send_url = 'https://web.whatsapp.com/send?'

    phones = pd.read_excel('./data/phones.xlsx')
    df = pd.read_excel('./data/results.xlsx')

    max_rows = df.shape[0]

    for index, row in phones.iterrows():
        message = get_message(row, unparsed_message)

        send_params = parse.urlencode(
            {'phone': row['Phone'], 'text': message})
        page.goto(send_url + send_params)

        expect(page.locator(
            '//*[@id="app"]/div/div[2]/div[3]/header/div[1]/div/img')).to_be_visible(timeout=50000)
        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)
        df_pos = max_rows + index
        if (not str(row['Phone']).startswith('+')):
            row['Phone'] = '+' + str(row["Phone"])

        if (invalid_number is True):
            print(f'{row['Phone']} is an invalid phone number')
            df.loc[df_pos, 'Phone'] = row['Phone']
            df.loc[df_pos, 'Date'] = datetime.now()
            df.loc[df_pos, 'Exists'] = False
            df.to_excel('./data/results.xlsx', index=False)
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

        df.loc[df_pos, 'Phone'] = row['Phone']
        df.loc[df_pos, 'Date'] = datetime.now()
        df.loc[df_pos, 'Exists'] = True

        df.to_excel('./data/results.xlsx', index=False)
        page.wait_for_timeout(WAIT_TIME)
