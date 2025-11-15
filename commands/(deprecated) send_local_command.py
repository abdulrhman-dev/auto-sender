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

    print(df)

    df['PHONE'] = df['PHONE'].astype(str)
    working_df = df[df['STATUS'] != True].head(args['COUNT'])

    print(working_df)

    page = browser.new_page()

    page.goto("https://web.whatsapp.com/")

    main_url = 'https://wa.me/'

    for index, row in working_df.iterrows():
        message = format_message(row, unparsed_message)

        send_params = row['CUS_MOBILE_1'] + '?' + parse.urlencode(
            {'text': message})

        send_url = main_url + send_params

        expect(page.locator(
            '//div[@id="side"]')).to_be_visible(timeout=50000)

        page.evaluate(f"""
                    var a = document.createElement('a');
                    var link = document.createTextNode("hiding");
                    a.appendChild(link);
                    a.href = "{send_url}";
                    document.head.appendChild(a);
                    a.click();
        """
                      )

        expect(page.locator(
            'xpath=//div[text()="Starting chat"]')).not_to_be_visible(timeout=50000)

        page.wait_for_timeout(1000)
        invalid_number = page.locator(
            '//div[text()="Phone number shared via url is invalid."]').is_visible(timeout=2500)

        print(invalid_number)

        if (invalid_number is True):
            print(f'{row['PHONE']} is an invalid phone number')

            df.loc[index, 'STATUS'] = 1

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

        df.loc[index, 'STATUS'] = 1

        page.wait_for_timeout(WAIT_TIME)

    df.to_excel('./data/phones_local.xlsx', index=False)
