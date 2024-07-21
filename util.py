import pandas as pd
import re
from playwright.sync_api import expect, Page
messages = []


def get_whatsapp_messages(page: Page, return_type=''):
    expect(page.locator(
        'xpath=//*[@id="main"]/div[3]/div/div[2]/div[2]/div/span')).not_to_be_visible(timeout=2000)
    page.keyboard.press('Home')
    page.wait_for_timeout(1500)

    messages = []

    for row in page.locator('xpath=//div[@role="application"]//div[@role="row"]').all():
        sender = 'out'

        if 'message-in' in row.locator('xpath=/div/div').get_attribute('class'):
            sender = 'in'

        if (return_type != '' and sender != return_type):
            continue

        inner_message_parts = row.locator(
            'xpath=//span[@class="_ao3e selectable-text copyable-text"]').all()

        inner_message = ''
        for inner_message_part in inner_message_parts:
            if (inner_message == ""):
                inner_message = inner_message_part.text_content()
                continue

            inner_message += '\n' + inner_message_part.text_content()

        if (inner_message == ''):
            continue

        messages.append(
            {'content': inner_message, 'sender': sender})
    return messages


def get_message(row: pd.Series, unparsed_message: str):
    message = unparsed_message

    for match in re.finditer(r'{{\s*(?P<column_name>\w+)\s*}}', unparsed_message):
        match_groups = match.groupdict()
        value = row[match_groups['column_name']]
        message = message.replace(match.group(0), value, 1)

    return message
