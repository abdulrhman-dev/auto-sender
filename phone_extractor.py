import pandas as pd

with open('./data/selected_phones.txt', 'r') as f:
    phones = f.read().split('\n')

df = pd.read_excel('./data/202407-08_alserag_data_analysis.xlsx')
print(phones)
working_df = df.loc[df['WhatsApp_url_1'].isin(
    phones), ['WhatsApp_url_1', 'Cust_name', 'Cust_title']]


working_df['Phone'] = working_df['WhatsApp_url_1'].str.replace(
    'https://wa.me/', '+')
working_df['Cust_title'] = working_df['Cust_title'].str.replace(
    r'\s+', '', regex=True)
working_df.drop('WhatsApp_url_1', axis=1, inplace=True)

is_adult = ~working_df['Cust_title'].isin(
    ['الطفل', 'الطفلة'])

working_df.loc[is_adult, 'Cust_name'] = working_df.loc[is_adult,
                                                       'Cust_name'].str.split(' ').str[0]
working_df.loc[~is_adult, 'Cust_name'] = working_df.loc[~is_adult,
                                                        'Cust_name'].str.split(' ').str[1]

working_df['Cust_title_1'] = working_df['Cust_title'].replace(
    {'السيد': 'الأستاذ', 'السيدة': 'الأستاذة', 'الطفل':'الأستاذ', 'الطفلة':'الأستاذ'})
working_df['Cust_title_2'] = working_df['Cust_title_1'].str.contains('ة').map({True:'عميلتنا المحترمة', False:'عميلنا المحترم'})
working_df['Cust_title_1'] = working_df['Cust_title_1'].str.replace('ال', '')
working_df.drop('Cust_title', axis=1, inplace=True)
working_df.to_excel('./data/phones.xlsx', index=False)
