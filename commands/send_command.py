from util import format_message, update_send_status
from urllib import parse
from datetime import datetime
import time
from os import getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
from waha import send_message
load_dotenv()


def execute(args):
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

    for _, row in df.iterrows():
        message = format_message(row, unparsed_message)
        phone_number = row['CUS_MOBILE_1']

        print(f'Sending message to {phone_number}')
        res = send_message(phone_number, message)

        if (res.status_code != 201):
            print(f'Faild to send message to {phone_number}')
            data = {
                'WHATASAPP_EXISTS': 0,
                'SEND_DATE': datetime.now(),
                'CUS_MOBILE_1': phone_number,
                'YEAR': YEAR,
                'MONTH': MONTH
            }
            update_send_status(data, cursor, conn)
            time.sleep(WAIT_TIME)
            continue

        data = {
            'WHATASAPP_EXISTS': 1,
            'SEND_DATE': datetime.now(),
            'CUS_MOBILE_1': phone_number,
            'YEAR': YEAR,
            'MONTH': MONTH
        }
        update_send_status(data, cursor, conn)
        print(f'Successfully sent message')
        time.sleep(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
