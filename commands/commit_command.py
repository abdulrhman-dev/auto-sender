import pandas as pd
from sqlalchemy import types, create_engine
from dotenv import load_dotenv
import os

load_dotenv()


def execute():
    file_path = os.path.join(os.getcwd(), 'edit', 'edit.xlsx')

    if (not os.path.exists(file_path)):
        raise Exception(
            'No new edits avaliable please run the edit command first before commiting')

    phone_engine = create_engine(os.getenv('PHONE_CON'))

    db_df = pd.read_sql('SELECT * FROM phonedb.customer_phones', phone_engine)
    sql_db = db_df.copy()

    edit_df = pd.read_excel(file_path)

    working_month = edit_df['MONTH'].unique()
    working_year = edit_df['YEAR'].unique()

    db_df.drop(db_df[db_df['YEAR'].isin(working_year) &
               db_df['MONTH'].isin(working_month)].index, inplace=True)
   
    save_df = pd.concat([db_df, edit_df])
    


    if (sql_db['INV_NO'].count() != save_df['INV_NO'].count()):
        raise Exception('Something went wrong with commiting database changes')

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

    save_df.to_sql('customer_phones', phone_engine,
                   if_exists='replace', dtype=dtype, index=False)
