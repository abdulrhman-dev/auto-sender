import pandas as pd
import re
from pandas import DataFrame, ExcelWriter
import mysql.connector
import json


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
