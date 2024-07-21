# import pywhatkit as wa
import re
import pandas as pd
import pywhatkit as wa
import random


with open('./data/message.txt', 'r', encoding='utf-8') as f:
    unparsed_message = f.read()


def apply_row(row: pd.Series):
    message = unparsed_message

    for match in re.finditer(r'{{\s*(?P<column_name>\w+)\s*}}', unparsed_message):
        match_groups = match.groupdict()
        start_index, end_index = match.span()
        value = row[match_groups['column_name']]
        message = message.replace(match.group(0), value, 1)

    return message


df = pd.read_excel('./data/phones.xlsx')


for index, row in df.iterrows():
    wait_time = random.randrange(15, 25)
    message = apply_row(row)
    print(message)
    if(not str(row['Phone']).startswith('+')):
        row['Phone'] = '+' + str(row['Phone'])
    wa.sendwhatmsg_instantly(str(row['Phone']), message, wait_time, True, 5)
    print(f'Sent Message Successfully')
