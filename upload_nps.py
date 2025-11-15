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
load_dotenv()

engine = create_engine(getenv('PHONE_CON'))

df = pd.read_excel("./upload_nps.xlsx", sheet_name='this')

df['SEND_DATE'] = pd.to_datetime(df['SEND_DATE'].astype(int), origin='1899-12-30', unit='D').dt.strftime('%Y-%m-%d %H:%M:%S')
print(df['SEND_DATE'])

conn = mysql.connector.connect(
    user=getenv('USER'),
    password=getenv('PASSWORD'),
    host=getenv('HOST')
)


cursor = conn.cursor()
first_enc = False
for index, row in df.iterrows():
    update_query = """
                        UPDATE phonedb.customer_phones
                        SET WHATSAPP_EXISTS = %s, RESPONDED = %s, RESPONSE = %s, NPS = %s, SEND_DATE = %s
                        WHERE (MONTH = %s AND YEAR = %s AND INV_NO = %s )
                  """
    if(row['SEND_DATE'] == "2025-04-25 00:00:00"):
        row['SEND_DATE'] = "2025-04-25 01:00:00"
    
    
    cursor.execute(update_query,
                   (
                       row["WHATSAPP_EXISTS"],
                       row["RESPONDED"],
                       row["RESPONSE"],
                       row["NPS"],
                       row["SEND_DATE"],
                       row["MONTH"],
                       row["YEAR"],
                       row["INV_NO"]
                   )
                   )

    conn.commit()

cursor.close()
conn.close()
