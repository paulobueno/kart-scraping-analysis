from collections import namedtuple

from tqdm import tqdm
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib.parse as urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup as bs
from datetime import timedelta, datetime
import pandas as pd
import os


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

    def __init__(self, date_range, circuit='granjaviana'):
        self.params_to_scrap = {
            'circuit': circuit,
            'domain': 'http://www.kartodromogranjaviana.com.br/resultados',
            'init': date_range[0],
            'end': date_range[1]
            }
        self.session = self.get_session()
        print('Configs loaded:\n', '\n '.join(
                [f'{k}: {v}' for k, v in self.params_to_scrap.items()]
                ))

    def __call__(self, *args, **kwargs):
        if kwargs.get('to_csv'):
            path = kwargs.get('to_csv')
            results = self.collect_all_results()
            results.to_csv(path, index=False, sep=';', decimal=',')
            print('Saved to CSV on:', path)

    def get_session(self):
        http = requests.Session()
        retries = Retry(total=3, backoff_factor=1,
                        status_forcelist=[429, 500, 502, 503, 504])
        http.mount('http://', HTTPAdapter(max_retries=retries))
        http.headers = self.my_headers
        return http

    def gen_params_list(self):
        params_list = []
        date_init = self.params_to_scrap.get('init')
        date_end = self.params_to_scrap.get('end')
        date_init = datetime.fromisoformat(date_init)
        date_end = datetime.fromisoformat(date_end)
        place = self.params_to_scrap.get('place')
        delta = date_end - date_init
        for i in range(delta.days + 1):
            _date = date_init + timedelta(days=i)
            params = {
                'flt_kartodromo': place,
                'flt_ano': _date.year,
                'flt_mes': _date.month,
                'flt_dia': _date.day,
                'flt_tipo': ''
                }
            params_list.append(params)
        return params_list

    def get_uids(self):
        params_list = self.gen_params_list()
        domain = self.params_to_scrap.get('domain')
        data = []
        print('-'*20, 'Collecting UIDs', '-'*20)
        for params in tqdm(params_list):
            page = self.session.get(domain, params=params)
            table_rows = bs(page.content, 'html.parser').table.select('tr')
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
        domain = self.params_to_scrap.get('domain') + '/folha'
        params = {'uid': uid, 'parte': 'prova'}
        page = self.session.get(domain, params=params)
        data = bs(page.content, 'html.parser').table.select('tr')
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
