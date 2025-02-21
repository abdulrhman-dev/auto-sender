import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from util import to_table
load_dotenv()


def execute(args):
    MONTH = args['MONTH']
    YEAR = args['YEAR']

    phone_engine = create_engine(os.getenv('PHONE_CON'))

    df = pd.read_sql(
        'SELECT * FROM phonedb.customer_phones', phone_engine)

    df = df[(df['YEAR'] == YEAR) & (df['MONTH'] == MONTH)].copy()

    df.sort_values(by='INV_TIME', inplace=True)
    
    edit_path = os.path.join(os.getcwd(), 'edit')
    if (not os.path.exists(edit_path)):
        os.makedirs(edit_path)
    with pd.ExcelWriter(os.path.join(edit_path, f'edit.xlsx')) as writer:
        to_table(df, 'Edit', writer)
        os.startfile(os.path.join(edit_path, f'edit.xlsx'))
