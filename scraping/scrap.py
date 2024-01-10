from collections import namedtuple
import urllib.parse as urlparse
from urllib.parse import parse_qs
import pandas as pd
from decouple import config
from bs4 import BeautifulSoup as Bs
from tqdm import tqdm
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class KgvCollectData:
    my_headers = {
        "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/71.0.3578.98 Safari/537.36",
        "Accept":
            "text/html,application/xhtml+xml,"
            "application/xml;"
            "q=0.9,"
            "image/webp,"
            "image/apng,"
            "*/*;q=0.8"
    }
    domain = 'https://kartodromogranjaviana.com.br'

    def __init__(self, date_range, debug=False, circuit='granjaviana'):
        self.params = {
            'circuit': circuit,
            'domain': self.domain,
            'init': date_range[0],
            'end': date_range[1]
        }
        self.DEBUG = debug
        if self.DEBUG:
            print('> DEBUG mode activated')
        print('> Configs loaded:', *[f' {k}: {v}' for k, v in self.params.items()], sep='\n')
        self.session = self.get_session()

    def get_cookies(self):
        username = config('KGV_USERNAME')
        password = config('KGV_PASSWORD')
        driver = webdriver.Firefox()
        driver.get(self.domain + '/member-login')
        driver.find_element(By.ID, 'swpm_user_name').send_keys(username)
        driver.find_element(By.ID, 'swpm_password').send_keys(password)
        WebDriverWait(driver, 30).until(
            EC.text_to_be_present_in_element(
                (By.CSS_SELECTOR, '.swpm-logged-username-value.swpm-logged-value'),
                username))
        cookies = driver.get_cookies()
        driver.quit()
        return cookies

    def get_session(self):
        print('> Creating session')
        s = requests.Session()
        for cookie in self.get_cookies():
            s.cookies.set(cookie['name'], cookie['value'])
        retries = Retry(total=3,
                        backoff_factor=1,
                        status_forcelist=[429, 500, 502, 503, 504])
        s.mount('https://', HTTPAdapter(max_retries=retries))
        s.headers = self.my_headers
        print('> Session created')
        return s

    def gen_query_params_list(self):
        query_params_list = []
        date_init = self.params.get('init')
        date_end = self.params.get('end')
        circuit = self.params.get('circuit')
        date_init = datetime.fromisoformat(date_init)
        date_end = datetime.fromisoformat(date_end)
        delta = date_end - date_init
        for i in range(delta.days + 1):
            _date = date_init + timedelta(days=i)
            params = {
                'flt_kartodromo': circuit,
                'flt_ano': _date.year,
                'flt_mes': _date.month,
                'flt_dia': _date.day,
                'flt_tipo': ''
            }
            query_params_list.append(params)
        return query_params_list

    def get_uids(self):
        params_list = self.gen_query_params_list()
        domain = self.params.get('domain') + '/resultados'
        data = []
        print('-' * 20, 'Collecting UIDs', '-' * 20)
        for params in tqdm(params_list):
            page = self.session.get(domain, params=params)
            soup = Bs(page.content, 'html.parser')

            # if self.DEBUG:
            #     load_page = input('\n> visualize page? y to yes: ')
            #     if load_page == 'y':
            #         with open('rendered_page.html', 'w', encoding='utf-8') as file:
            #             file.write(str(soup))
            #         webbrowser.open('rendered_page.html')

            table_rows = Bs(page.content, 'html.parser').table.select('tr')
            first_row = table_rows[0].select('th')[:4]
            label_columns = [column.text for column in first_row] + ['uid']
            data_point = namedtuple('Data', label_columns)
            for row in table_rows[1:]:
                url_result = [
                    column.get('href') for column in row.select('a')
                    if column.get('title') == 'Resultado'
                ]
                if len(url_result) > 0:
                    data_row = [column.text for column in row.select('td')[:4]]
                    parsed = urlparse.urlparse(url_result[0])
                    data_row.extend(parse_qs(parsed.query)['uid'])
                    data.append(data_point(*data_row))
                else:
                    continue
        return data

    def collect_uid_results(self, uid):
        domain = self.params.get('domain') + '/folha'
        params = {'uid': uid, 'parte': 'prova'}
        page = self.session.get(domain, params=params)
        data = Bs(page.content, 'html.parser').table.select('tr')

        # if self.DEBUG:
        #     load_page = input('\n> visualize page? y to yes: ')
        #     if load_page == 'y':
        #         with open('rendered_page.html', 'w', encoding='utf-8') as file:
        #             file.write(str(data))
        #         webbrowser.open('rendered_page.html')

        column_labels = [column.text for column in data[0].select('th')]
        all_data = [
            [column.text for column in columns.select('td')]
            for columns in data[1:]
        ]
        return [dict(zip(column_labels, values)) for values in all_data]

    def collect_all_results(self):
        uids_list = self.get_uids()
        all_data = []
        print('-' * 20, 'Collecting Results', '-' * 20)
        for uid_data in tqdm(uids_list):
            result = self.collect_uid_results(uid_data.uid)
            result = [
                {**uid_data._asdict(), **dict_data} for dict_data in result
            ]
            all_data.extend(result)
        return pd.DataFrame(all_data)

    def save_results(self, path):
        results = self.collect_all_results()
        results.to_csv(path, index=False, sep=';', decimal=',')
        print('Saved to CSV on:', path)


if __name__ == '__main__':
    DEBUG = config('DEBUG', default=False, cast=bool)
    KgvCollectData(('2022-01-01', '2022-01-05'), debug=DEBUG).save_results('../Data/data.csv')
