import pandas as pd
from sqlalchemy import create_engine
from os import getenv
from dotenv import load_dotenv
from datetime import date
import os
load_dotenv()


def execute(args):
    MONTH = args['MONTH']
    YEAR = args['YEAR']

    phone_engine = create_engine(getenv('PHONE_CON'))

    df = pd.read_sql(
        'SELECT * FROM phonedb.customer_phones', phone_engine)

    df = df[(df['YEAR'] == YEAR) & (df['MONTH'] == MONTH)].copy()

    if (df.loc[df['RESPONDED'] == 1, 'CUS_MOBILE_1'].count() == 0):
        raise Exception('No customer has replayed yet')

    whatsapp_sent_count = df.loc[~df['SEND_DATE'].isna(), 'CUS_MOBILE_1'].count()
    all_customers_count = df['CUS_MOBILE_1'].count(
    )
    
    status = f'For {whatsapp_sent_count} out of {all_customers_count} customers'
    report = pd.DataFrame(
        columns=pd.MultiIndex.from_tuples([('RESPONSE', 'COUNT'), ('RESPONSE', 'PERCENTAGE'), ('NPS', 'AVG')]))

    report.loc[status, [('RESPONSE', 'COUNT')]
               ] = df.loc[df['RESPONDED'] == 1, 'CUS_MOBILE_1'].count()
    response_percentage = df.loc[df['RESPONDED'] == 1, 'CUS_MOBILE_1'].count(
    ) / df.loc[~df['SEND_DATE'].isna(), 'CUS_MOBILE_1'].count()
    report.loc[status, [('RESPONSE', 'PERCENTAGE')]
               ] = '{:.0%}'.format(response_percentage)
    report.loc[status, [('NPS', 'AVG')]] = df['NPS'].mean()

    folder_path = os.path.join(os.getcwd(), 'report')
    if (not os.path.exists(folder_path)):
        os.makedirs(folder_path)
    
    report_path = os.path.join(folder_path, f'{date.today().strftime('%Y%m-%d')}response_report.xlsx')
    
    report.to_excel(report_path)
    os.startfile(report_path)
