from util import update_nps
from dotenv import load_dotenv
from os import getenv
from sqlalchemy import create_engine
import mysql.connector
import re
import pandas as pd
import sys
from waha import get_messages
import time
sys.path.append('../')


load_dotenv()


def execute(args):
    MONTH = args['MONTH']
    YEAR = args['YEAR']
    COUNT = args['COUNT']
    WAIT_TIME = int(getenv('WAIT_TIME'))

    engine = create_engine(getenv('PHONE_CON'))
    query = f"""
        SELECT *
        FROM phonedb.customer_phones
        WHERE (MONTH = {MONTH} AND YEAR = {YEAR} AND WHATSAPP_EXISTS = 1)
        ORDER BY INV_TIME LIMIT {COUNT}
    """

    df = pd.read_sql(query, engine)

    conn = mysql.connector.connect(
        user=getenv('USER'),
        password=getenv('PASSWORD'),
        host=getenv('HOST')
    )

    cursor = conn.cursor()

    recorded = 0
    total = 0

    for _, row in df.iterrows():
        total += 1
        phone_number = row['CUS_MOBILE_1']
        res = get_messages(phone_number)
        print(f'Getting NPS Score for {phone_number}')

        if (res.status_code != 200):
            print(
                f'Failed to get NPS score for {phone_number}')
            data = {
                'RESPONDED': 0,
                'RESPONSE': None,
                'NPS': None,
                'MONTH': MONTH,
                'YEAR': YEAR,
                'CUS_MOBILE_1': phone_number
            }
            update_nps(data, cursor, conn)
            time.sleep(WAIT_TIME)
            print(f'recorded {recorded} out of {total}')
            continue

        messages = res.json()
        found_nps = False

        if (len(messages) == 0):
            data = {
                'RESPONDED': 0,
                'RESPONSE': None,
                'NPS': None,
                'MONTH': MONTH,
                'YEAR': YEAR,
                'CUS_MOBILE_1': phone_number
            }
            update_nps(data, cursor, conn)

        for message in messages:
            if (message['fromMe']):
                data = {
                    'RESPONDED': 0,
                    'RESPONSE': None,
                    'NPS': None,
                    'MONTH': MONTH,
                    'YEAR': YEAR,
                    'CUS_MOBILE_1': phone_number
                }
                update_nps(data, cursor, conn)
                break

            matches = re.findall(
                r'([0-9]+|[\u0660-\u0669]+)', message['body'])

            if matches:
                nps = min([int(match) for match in matches])
                if (nps < 0):
                    print(f'recieved number is not a valid NPS, {nps}')
                    continue
                print(f'Got NPS Score for {phone_number}')
                print(f'NPS = {nps}')
                data = {
                    'RESPONDED': 1,
                    'RESPONSE': message['body'],
                    'NPS': nps,
                    'MONTH': MONTH,
                    'YEAR': YEAR,
                    'CUS_MOBILE_1': phone_number
                }
                update_nps(data, cursor, conn)
                recorded += 1
                found_nps = True
                break

        if (not found_nps):
            print(f'Failed to get NPS score for {phone_number}')
        print(f'recorded {recorded} out of {total}')

        time.sleep(WAIT_TIME)

    conn.commit()
    cursor.close()
    conn.close()
