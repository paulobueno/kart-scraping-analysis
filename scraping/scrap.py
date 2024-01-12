import sqlite3
from collections import namedtuple
import urllib.parse as urlparse
from urllib.parse import parse_qs
from decouple import config
from bs4 import BeautifulSoup as Bs
import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class KgvScraper:
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

    def __init__(self):
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

    def get_uids_from_page(self, params):
        domain = self.domain + '/resultados'
        page = self.session.get(domain, params=params)
        table_rows = Bs(page.content, 'html.parser').table.select('tr')
        first_row = table_rows[0].select('th')[:4]
        label_columns = [column.text for column in first_row] + ['uid']
        data = []
        for row in table_rows[1:]:
            url_result = [
                column.get('href') for column in row.select('a')
                if column.get('title') == 'Resultado'
            ]
            if len(url_result) > 0:
                data_row = [column.text for column in row.select('td')[:4]]
                parsed = urlparse.urlparse(url_result[0])
                data_row.extend(parse_qs(parsed.query)['uid'])
                data.append(dict(zip(label_columns, data_row)))
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


class DataBase:
    def __init__(self):
        self.conn = sqlite3.connect('scraping_data.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS params_to_scrap (
                id INTEGER PRIMARY KEY,
                circuit TEXT,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                race_type TEXT,
                fetched BOOLEAN,
                UNIQUE(circuit, year, month, day, race_type)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS bronze_racing_tries (
                id INTEGER PRIMARY KEY,
                day TEXT,
                time TEXT,
                category TEXT,
                title TEXT,
                uid TEXT,
                fetched BOOLEAN,
                UNIQUE(uid)
            )
        ''')
        self.conn.commit()

    def insert_params_data(self, param_record):
        try:
            self.cursor.execute(
                '''INSERT INTO params_to_scrap 
                (circuit, year, month, day, race_type, fetched) 
                VALUES (?, ?, ?, ?, ?, ?)''',
                param_record
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            self.conn.rollback()

    def insert_racing_tries_list(self, racing_tries_list):
        for racing_try in racing_tries_list:
            try:
                self.cursor.execute(
                    '''INSERT INTO bronze_racing_tries 
                    (day, time, category, title, uid, fetched) 
                    VALUES (?, ?, ?, ?, ?, False)''',
                    tuple(racing_try.get(column) for column in ['day', 'time', 'category', 'title', 'uid'])
                )
                self.conn.commit()
            except sqlite3.IntegrityError:
                self.conn.rollback()

    def set_params_as_fetched(self, row_id):
        self.cursor.execute("UPDATE params_to_scrap SET fetched = ? WHERE ID = ?", (True, row_id))
        self.conn.commit()

    def get_first_not_fetched(self):
        self.cursor.execute('SELECT id, circuit, year, month, day FROM params_to_scrap WHERE fetched is False LIMIT 1')
        return self.cursor.fetchone()


def gen_query_params_list(init, end, circuit='granjaviana', race_type=''):
    param_record = namedtuple('param_record', ['circuit', 'year', 'month', 'day', 'race_type', 'fetched'])
    param_records = []
    date_init = datetime.fromisoformat(init)
    date_end = datetime.fromisoformat(end)
    delta = date_end - date_init
    for i in range(delta.days + 1):
        current_date = date_init + timedelta(days=i)
        param_records.append(param_record(circuit,
                                          current_date.year,
                                          current_date.month,
                                          current_date.day,
                                          race_type,
                                          False))
    return param_records


def translate_each_dict(list_of_dicts, translation_dict):
    def translate_dict(element):
        return {translation_dict.get(key): value
                for key, value in element.items()
                if key in translation_dict.keys()}

    return [translate_dict(_dict) for _dict in list_of_dicts]


def main(init, end):
    params_list = gen_query_params_list(init, end)
    scraper = KgvScraper()
    db = DataBase()

    for params in params_list:
        db.insert_params_data(params)

    while db.get_first_not_fetched():
        query_params = dict(zip(['id', 'flt_kartodromo', 'flt_ano', 'flt_mes', 'flt_dia'],
                                db.get_first_not_fetched()))
        data = scraper.get_uids_from_page(query_params)
        data = translate_each_dict(data, {'Dia': 'day',
                                          'Horario': 'time',
                                          'Categoria': 'category',
                                          'Título': 'title',
                                          'uid': 'uid'})
        db.insert_racing_tries_list(data)
        print('here > ', *data, sep='\n')
        db.set_params_as_fetched(query_params['id'])


if __name__ == '__main__':
    DEBUG = config('DEBUG', default=False, cast=bool)
    # KgvCollectData(('2022-01-01', '2022-01-05'), debug=DEBUG).save_results('../Data/data.csv')
    # print(*gen_query_params_list('2022-01-01', '2022-01-05'), sep='\n')
    main('2022-02-01', '2022-02-05')
    # db = DataBase()
    # print(db.get_first_not_fetched())
