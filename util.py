import pandas as pd
import re
from playwright.sync_api import expect, Page
from pandas import DataFrame, ExcelWriter
import mysql.connector
import json
messages = []


def get_whatsapp_messages(page: Page, return_type=''):
    try:
        expect(page.locator(
            'xpath=//*[@id="main"]/div[3]/div/div[2]/div[2]/div/span')).not_to_be_visible(timeout=7500)
    except:
        print("Had a problem with finding whatsapp's loading animation")
    page.keyboard.press('Home')
    page.wait_for_timeout(1500)

    messages = []
    rows = page.locator('xpath=//*[@id="main"]/div[2]/div/div[2]//div[@role="row"]').all()
    for row in rows:
        sender = 'out'

        if 'message-in' in row.locator('xpath=/div/div/div/div').get_attribute('class'):
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


def format_message(row: pd.Series, unparsed_message: str):
    with open('./extra_text.json', encoding='utf8') as f:
        extra_text = json.load(f)
    message = unparsed_message

    for match in re.finditer(r'{{\s*(?P<column_name>\w+)\s*}}', unparsed_message):
        match_groups = match.groupdict()
        column_name = match_groups['column_name']
        if column_name in extra_text:
            extra_column = extra_text[column_name]
            if (isinstance(extra_text['TEXT_1'], list)):
                value = extra_column[0 if row['CUS_GENDER'] == 'Male' else 1]
            else:
                value = extra_column
        else:
            value = row[column_name]
        message = message.replace(match.group(0), value, 1)

    return message


def update_send_status(data, cursor: mysql.connector.connect, conn: mysql.connector.connect):
    update_query = """
                    UPDATE phonedb.customer_phones 
                    SET WHATSAPP_EXISTS = %s, SEND_DATE = %s
                    WHERE (MONTH = %s AND YEAR = %s AND CUS_MOBILE_1 = %s)
                """
    cursor.execute(update_query, (data['WHATASAPP_EXISTS'],
                                  data['SEND_DATE'], data['MONTH'], data['YEAR'], data['CUS_MOBILE_1']))
    conn.commit()


def update_nps(data, cursor: mysql.connector.connect, conn: mysql.connector.connect):
    update_query = """
                    UPDATE phonedb.customer_phones 
                    SET RESPONDED = %s, RESPONSE = %s, NPS = %s
                    WHERE (MONTH = %s AND YEAR = %s AND CUS_MOBILE_1 = %s)
                """
    cursor.execute(update_query,
                   (
                       data['RESPONDED'],
                       data['RESPONSE'], data['NPS'],
                       data['MONTH'], data['YEAR'],
                       data['CUS_MOBILE_1']
                   )
                   )
    conn.commit()


def to_table(df: DataFrame, sheet_name: str, writer: ExcelWriter):
    df.to_excel(writer, sheet_name=sheet_name,
                startrow=1, header=False, index=False)

    worksheet = writer.sheets[sheet_name]

    (max_row, max_col) = df.shape

    column_settings = [{"header": column} for column in df.columns]

    worksheet.add_table(0, 0, max_row, max_col - 1,
                        {'columns': column_settings})

    worksheet.set_column(0, max_col - 1, 12)
