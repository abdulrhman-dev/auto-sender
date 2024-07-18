# import pywhatkit as wa
import re
import pandas as pd
import pywhatkit as wa
import random


with open('./data/message.txt', 'r') as f:
    unparsed_message = f.read()


def apply_row(row: pd.Series):
    message = unparsed_message
    shift_value = 0

    for match in re.finditer(r'{{\s*(?P<column_name>\w+)\s*}}', unparsed_message):
        match_groups = match.groupdict()
        start_index, end_index = match.span()
        value = row[match_groups['column_name']]
        before_value_slice = slice(None, start_index + shift_value)
        after_value_slice = slice(end_index + shift_value, -1)
        message = message[before_value_slice] + \
            value + message[after_value_slice]
        shift_value += len(value) - len(match.group(0))

    return message


df = pd.read_excel('./phones.xlsx')


for index, row in df.iterrows():
    wait_time = random.randrange(6, 20)
    row['Shop'] = "Al Abood Shop"
    message = apply_row(row)
    wa.sendwhatmsg_instantly(
        '+' + str(row['Phone']), message, wait_time, True, 1)
    print(f'Sent Message Successfully')
