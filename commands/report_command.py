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

    status = f'For {df['CUS_MOBILE_1'].count(
    )} out of {df.loc[~df['SEND_DATE'].isna(), 'CUS_MOBILE_1'].count()} customers'
    report = pd.DataFrame(
        columns=pd.MultiIndex.from_tuples([('RESPONSE', 'COUNT'), ('RESPONSE', 'PERCENTAGE'), ('NPS', 'AVG')]))

    report.loc[status, [('RESPONSE', 'COUNT')]
               ] = df.loc[df['RESPONDED'] == 1, 'CUS_MOBILE_1'].count()
    response_percentage = df.loc[df['RESPONDED'] == 1, 'CUS_MOBILE_1'].count(
    ) / df.loc[~df['SEND_DATE'].isna(), 'CUS_MOBILE_1'].count()
    report.loc[status, [('RESPONSE', 'PERCENTAGE')]
               ] = '{:.0%}'.format(response_percentage)
    report.loc[status, [('NPS', 'AVG')]] = df['NPS'].mean()

    report_path = os.path.join(os.getcwd(), 'report')
    if (not os.path.exists(report_path)):
        os.makedirs(report_path)
    report.to_excel(
        os.path.join(report_path, f'{date.today().strftime('%Y%m-%d')}response_report.xlsx'))
