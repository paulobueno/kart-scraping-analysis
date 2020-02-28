import requests
from bs4 import BeautifulSoup as bs
from datetime import date, timedelta, datetime
import pandas as pd
from os import system


class KgvCollectData:
    url_params_list = []
    url_list = []
    collected_data = []
    collected_data_pandas = pd.DataFrame()
    session = requests.Session()
    params_to_scrap = {
        'place': 'granjaviana',
        'domain': 'http://www.kartodromogranjaviana.com.br/resultados',
        'init': date.today() - timedelta(days=180),
        'end': date.today()
    }
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

    def __init__(self):
        pass

    def date_filter(self, date_init, date_end):
        self.params_to_scrap.update({
            'init': datetime.strptime(date_init, '%Y-%m-%d'),
            'end': datetime.strptime(date_end, '%Y-%m-%d')
        })

    def gen_params_list(self):
        del self.url_params_list[:]
        date_init = self.params_to_scrap.get('init')
        date_end = self.params_to_scrap.get('end')
        delta = date_end - date_init
        for i in range(delta.days + 1):
            date = date_init + timedelta(days=i)
            params = {
                'flt_kartodromo': self.params_to_scrap.get('place'),
                'flt_ano': date.year,
                'flt_mes': date.month,
                'flt_dia': date.day,
                'flt_tipo': ''
            }
            self.url_params_list.append(params)

    def gen_results_urls(self):
        del self.url_list[:]
        system('clear')
        print('----------------- Collecting URLs ------------------')
        for i, params in enumerate(self.url_params_list):
            print('\r' + str(i + 1) + ' pages process of ' + str(len(self.url_params_list)), end='')
            page = self.session.get(
                self.params_to_scrap.get('domain'),
                headers=self.my_headers,
                params=params
            )
            soup = bs(page.content, 'html.parser')
            for link in soup.find_all('a'):
                if 'prova' in link.get('href'):
                    self.url_list.append(link.get('href'))
        self.url_list = [x.replace('.', '') for x in self.url_list]
        del self.url_params_list[:]
        print(': DONE')

    def collect_results(self):
        del self.collected_data[:]
        print('----------------- Collecting Results ------------------')
        for i, url in enumerate(self.url_list):
            print('\r' + str(i + 1) + ' of ' + str(len(self.url_list)), end='')
            page = self.session.get(
                self.params_to_scrap.get('domain') + url,
                headers=self.my_headers
            )
            data = bs(page.content, 'html.parser').table
            clean_race_data = [[x.get_text() for x in data.select('th')]]
            for race_data in data.select('tr')[1:]:
                clean_racer_data = [x.get_text() for x in race_data.select('td')]
                clean_race_data.append(clean_racer_data)
            self.collected_data.append(clean_race_data)
        del self.url_list[:]
        print(': DONE')

    def save_results(self, file_name):
        print('----------------- Saving Results ------------------')
        for race in self.collected_data:
            _df = pd.DataFrame(race[1:], columns=race[0])
            self.collected_data_pandas = self.collected_data_pandas.append(_df, ignore_index=True)
        self.collected_data_pandas.to_csv(file_name, index=False)
        print('DONE')


if __name__ == '__main__':
    kgv_data = KgvCollectData()
    kgv_data.date_filter('2019-01-01', '2019-12-31')
    kgv_data.gen_params_list()
    kgv_data.gen_results_urls()
    kgv_data.collect_results()
    kgv_data.save_results('Data/kart_data.csv')
