import os
import requests
import time, random
import pandas as pd
from bs4 import BeautifulSoup


def part_1():
    url = r'https://www.ifb.ir/YTM.aspx'
    header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
            }

    r = requests.get(url, headers=header)
    tables = pd.read_html(r.text, extract_links = "body")

    fixed_income_bonds_table = tables[2]
    downloaded_links = fixed_income_bonds_table['نماد'].apply(lambda x: x[1])
    new_links = [f'https://www.ifb.ir/{link}' for link in downloaded_links]

    new_dataframe = fixed_income_bonds_table.applymap(lambda x: x[0])
    new_dataframe['link'] = new_links

    new_dataframe.to_csv('part_1.csv', sep='\t', encoding='utf-16', index=False)


def part_2():
    part_1_df = pd.read_csv('part_1.csv', encoding='utf-16', sep='\t')

    for _, row in part_1_df.iterrows():

        initial_page, name = str(row['link']), str(row['نماد'])

        initial_page = initial_page.replace('Instruments', 'Instrumentsmfi')
        session = requests.Session()

        page = 1
        current_dataframe = None

        print(f'scrapping [{initial_page}] started.')

        while True:
            post_data = {
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$grdPBs',
                '__EVENTARGUMENT': f'Page${page}',
                }

            response = session.post(initial_page, data=post_data)
            soup = BeautifulSoup(response.content, 'html.parser')
            data = soup.find_all('table', class_='mGrid') 
            table_i_want = data[1]

            if 'داده ای یافت نشد' in str(table_i_want):
                break
            else:
                data_frames = pd.read_html(str(table_i_want))
                first_df = data_frames[0]
                df_filtered = first_df[~first_df['مبلغ سود هر ورقه'].str.contains('تعداد رکورد')]
                current_dataframe = df_filtered.copy() if page == 1 else pd.concat([current_dataframe, df_filtered], ignore_index=True)

            wait_time = random.randint(3, 7)
            print(f'Waiting {wait_time} seconds to go Page {page}')
            time.sleep(wait_time)
            page += 1
            print(current_dataframe)

        print(f'scrapping [{initial_page}] was done.')
        print('==' * 10)

        if current_dataframe is not None:
            current_dataframe = current_dataframe.rename(columns={'تاریخ': 'Date', 'مبلغ سود هر ورقه': 'CouponRate'})
    
            current_dataframe['BondName'] = name
            current_dataframe.to_csv(rf'part_2\part_2_{name}.csv', sep='\t', encoding='utf-16', index=False)

        time.sleep(random.randint(5, 10))


def final_merge_csv_files(folder_path, output_file):
    all_data = []

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            data = pd.read_csv(file_path, sep='\t', encoding='utf-16')
            all_data.append(data)

    merged_data = pd.concat(all_data, ignore_index=True)

    merged_data.to_csv(output_file, index=False, sep='\t', encoding='utf-16')


part_1()
part_2()
final_merge_csv_files('part_2', 'final_result.csv')
