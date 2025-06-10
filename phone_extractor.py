import pandas as pd
import numpy as np
import mysql.connector
import json
from sqlalchemy import create_engine, types
from os import getenv
from dotenv import load_dotenv

with open("./blacklist.json") as f:
    blacklist = json.load(f)

load_dotenv()

engine = create_engine(getenv('SAVE_CON'))

customers_df = pd.read_sql('SELECT * FROM workdb.customers', engine)
invoices_df = pd.read_sql('SELECT * FROM workdb.invoices', engine)

merged_df = pd.merge(customers_df, invoices_df, left_on='CUS_NO',
                     right_on='INV_CUS_NO', how='left')

merged_df['MONTH'] = merged_df['INV_TIME'].dt.month
merged_df['YEAR'] = merged_df['INV_TIME'].dt.year


mobile_mask = ~(merged_df['CUS_MOBILE_1'].str.startswith(
    'UnknownPhone')) & (merged_df['CUS_MOBILE_1'] != '') & (~merged_df['CUS_MOBILE_1'].isin(blacklist['PHONE_NUMBERS']))
df_mask = (merged_df['INV_CANCEL'] == 0) & mobile_mask


SELECTED_COLUMNS = ['INV_NO', 'INV_TIME', 'MONTH', 'YEAR', 'CUS_TITLE', 'CUS_NAME', 'CUS_JOB',
                    'CUS_GENDER', 'CUS_AGE', 'CUS_MOBILE_1', 'CUS_MOBILE_2', 'CUS_MOBILE_3']

new_customers_df = merged_df.loc[df_mask, SELECTED_COLUMNS].copy()
# new_customers_df = pd.read_excel('./data/TEST_CUSTOMER.xlsx')
new_customers_df.drop_duplicates(
    subset=['YEAR', 'MONTH', 'CUS_MOBILE_1'], keep='first', inplace=True)

is_adult_mask = ~new_customers_df['CUS_TITLE'].isin(
    ['الطفل', 'الطفلة'])
new_customers_df.loc[is_adult_mask,
                     'CUS_NAME'] = new_customers_df['CUS_NAME'].str.split(' ').str[0]
new_customers_df.loc[~is_adult_mask,
                     'CUS_NAME'] = new_customers_df['CUS_NAME'].str.split(' ').str[1]

new_customers_df['CUS_TITLE'] = new_customers_df['CUS_TITLE'].replace(
    {
        'السيد': 'الأستاذ',
        'السيدة': 'الأستاذة',
        'الطفل': 'الأستاذ',
        'الطفلة': 'الأستاذ',
        'الأنسة': 'الأستاذة',
        'الحاجة': 'الأستاذة',
        'العميد': 'الأستاذ',
        'النائب': 'الأستاذ',
        'الحاج': 'الأستاذ',
        'المقدم': 'الأستاذ',
        'العقيد': 'الأستاذ',
        'الملازم': 'الأستاذ'
    })
new_customers_df['CUS_TITLE'] = new_customers_df['CUS_TITLE'].str.replace(
    'ال', '')

new_customers_df['WHATSAPP_EXISTS'] = False
new_customers_df['SEND_DATE'] = np.nan
new_customers_df['RESPONDED'] = np.nan
new_customers_df['RESPONSE'] = ''
new_customers_df['NPS'] = np.nan

new_customers_df['CUS_MOBILE_1'] = pd.to_numeric(
    new_customers_df['CUS_MOBILE_1'])

new_customers_df['SEND_DATE'] = pd.to_datetime(new_customers_df['SEND_DATE'])
new_customers_df['CUS_AGE'] = new_customers_df['CUS_AGE'].replace(
    '', np.nan).replace(' ', np.nan).astype(float)

new_customers_df.set_index(['YEAR', 'MONTH', 'CUS_MOBILE_1'], inplace=True)

conn = mysql.connector.connect(
    user=getenv('USER'),
    password=getenv('PASSWORD'),
    host=getenv('HOST')
)


cursor = conn.cursor()

cursor.execute('CREATE DATABASE IF NOT EXISTS phonedb')
conn.database = 'phonedb'

cursor.execute(
    """CREATE TABLE IF NOT EXISTS customer_phones (
        INV_NO INTEGER,
        INV_TIME TIMESTAMP,
        MONTH INTEGER,
        YEAR INTEGER,
        CUS_TITLE VARCHAR(255),
        CUS_NAME VARCHAR(255),
        CUS_JOB VARCHAR(255),
        CUS_GENDER VARCHAR(255),
        CUS_AGE FLOAT,
        CUS_MOBILE_1 VARCHAR(255),
        CUS_MOBILE_2 VARCHAR(255),
        CUS_MOBILE_3 VARCHAR(255),
        WHATSAPP_EXISTS INTEGER,
        SEND_DATE TIMESTAMP,
        RESPONDED BOOLEAN ,
        RESPONSE VARCHAR(1024) ,
        NPS INTEGER
    )"""
)

conn.commit()
cursor.close()
conn.close()

phone_engine = create_engine(getenv('PHONE_CON'))

saved_df = pd.read_sql('SELECT * FROM phonedb.customer_phones', phone_engine)

saved_df['CUS_MOBILE_1'] = pd.to_numeric(saved_df['CUS_MOBILE_1'])
saved_df['YEAR'] = pd.to_numeric(saved_df['YEAR'])
saved_df['MONTH'] = pd.to_numeric(saved_df['MONTH'])

saved_df.set_index(['YEAR', 'MONTH', 'CUS_MOBILE_1'], inplace=True)

df = pd.concat([saved_df, new_customers_df])

df.drop(saved_df.index, inplace=True)
df.reset_index(inplace=True)

dtype = {
    'INV_NO': types.INTEGER,
    'INV_TIME': types.TIMESTAMP,
    'MONTH': types.INTEGER,
    'YEAR': types.INTEGER,
    'CUS_TITLE': types.VARCHAR(255),
    'CUS_NAME': types.VARCHAR(255),
    'CUS_JOB': types.VARCHAR(255),
    'CUS_GENDER': types.VARCHAR(255),
    'CUS_AGE': types.FLOAT,
    'CUS_MOBILE_1': types.VARCHAR(255),
    'CUS_MOBILE_2': types.VARCHAR(255),
    'CUS_MOBILE_3': types.VARCHAR(255),
    'WHATSAPP_EXISTS': types.INTEGER,
    'SEND_DATE': types.TIMESTAMP,
    'RESPONDED': types.BOOLEAN,
    'RESPONSE': types.VARCHAR(1024),
    'NPS': types.INTEGER
}

df.to_sql('customer_phones', phone_engine,
          if_exists='append', dtype=dtype, index=False)
