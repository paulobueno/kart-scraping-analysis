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

    def get_try_results_by_uid(self, uid):
        domain = self.domain + '/folha'
        params = {'uid': uid, 'parte': 'prova'}
        page = self.session.get(domain, params=params)
        all_data = Bs(page.content, 'html.parser')
        title_div = all_data.find('div', class_='headerbig')
        if title_div is None:
            return None
        title = title_div.text
        if len(title.split(' - ')) > 1:
            track = title.split(' - ')[1]
        elif title.find('CIRCUITO') != -1:
            track = title[title.find('CIRCUITO'):]
        else:
            track = title
        data = all_data.table.select('tr')

        # with open('rendered_page.html', 'w', encoding='utf-8') as file:
        #     file.write(str(all_data))

        column_labels = [column.text for column in data[0].select('th')]
        all_data = [
            [column.text for column in columns.select('td')]
            for columns in data[1:]
        ]
        return track, [dict(zip(column_labels, values)) for values in all_data]


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
                params_id INTEGER,
                day TEXT,
                time TEXT,
                category TEXT,
                track TEXT,
                title TEXT,
                uid TEXT,
                fetched BOOLEAN,
                UNIQUE(uid),
                FOREIGN KEY (params_id) REFERENCES params_to_scrap(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS bronze_try_results (
                id INTEGER PRIMARY KEY,
                uid TEXT,
                position TEXT, 
                car_number TEXT, 
                name TEXT, 
                class TEXT, 
                comment TEXT, 
                laps TEXT, 
                total_time TEXT, 
                best_lap_time TEXT, 
                total_gap TEXT, 
                gap TEXT, 
                UNIQUE(id),
                FOREIGN KEY (uid) REFERENCES bronze_racing_tries(uid)
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
                    (day, time, category, title, uid, params_id, fetched) 
                    VALUES (?, ?, ?, ?, ?, ?, False)''',
                    tuple(racing_try.get(column) for column in ['day', 'time', 'category', 'title', 'uid', 'params_id'])
                )
                self.conn.commit()
            except sqlite3.IntegrityError:
                self.conn.rollback()

    def set_params_as_fetched(self, table_name, row_id):
        self.cursor.execute(f"UPDATE {table_name} SET fetched = ? WHERE ID = ?", (True, row_id))
        self.conn.commit()

    def get_first_not_fetched(self, table_name, columns=None):
        if not columns:
            columns = ['*']
        self.cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name} WHERE fetched is False LIMIT 1")
        return self.cursor.fetchone()

    def get_all_not_fetched(self, table_name, columns=None):
        if not columns:
            columns = ['*']
        self.cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name} WHERE fetched is False")
        return self.cursor.fetchall()

    def count_not_fetched_registers_in_table(self, table_name):
        self.cursor.execute(f'SELECT count(1) FROM {table_name} WHERE fetched is False LIMIT 1')
        return self.cursor.fetchone()[0]

    def update_track_in_racing_tries(self, row_id, track):
        self.cursor.execute(f"UPDATE bronze_racing_tries SET track = ? WHERE ID = ?", (track, row_id))
        self.conn.commit()

    def insert_try_results(self, try_results_list):
        for try_result in try_results_list:
            try:
                columns = ['position', 'car_number', 'name', 'class',
                           'comment', 'laps', 'total_time', 'best_lap_time',
                           'total_gap', 'gap', 'uid']
                self.cursor.execute(
                    '''INSERT INTO bronze_try_results 
                    (position, car_number, name, class, comment, laps, total_time, best_lap_time, total_gap, gap, uid) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    tuple(try_result.get(column) for column in columns)
                )
                self.conn.commit()
            except sqlite3.IntegrityError:
                self.conn.rollback()


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


def translate_dicts_in_list(list_of_dicts, translation_dict):
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

    while db.get_first_not_fetched('params_to_scrap'):
        row_data = db.get_first_not_fetched('params_to_scrap', ['id', 'circuit', 'year', 'month', 'day'])
        row_id = row_data[0]
        query_params = dict(zip(['flt_kartodromo', 'flt_ano', 'flt_mes', 'flt_dia'], row_data[1:]))
        data = scraper.get_uids_from_page(query_params)
        for result in data:
            result['params_id'] = row_id
        data = translate_dicts_in_list(data, {'Dia': 'day',
                                              'Horario': 'time',
                                              'Categoria': 'category',
                                              'Título': 'title',
                                              'uid': 'uid',
                                              'params_id': 'params_id'})
        db.insert_racing_tries_list(data)
        db.set_params_as_fetched('params_to_scrap', row_id)

    all_racing_tries = db.get_all_not_fetched('bronze_racing_tries', ['id', 'uid'])
    for racing_try in all_racing_tries:
        row_id = racing_try[0]
        uid = racing_try[1]
        scraped_data = scraper.get_try_results_by_uid(uid)
        if scraped_data is None:
            continue
        track, data = scraped_data
        for result in data:
            result['uid'] = uid
        data = translate_dicts_in_list(data, {'Pos': 'position',
                                              'No.': 'car_number',
                                              'Nome': 'name',
                                              'Classe': 'class',
                                              'Comentários': 'comment',
                                              'Voltas': 'laps',
                                              'Total Tempo': 'total_time',
                                              'Melhor Tempo': 'best_lap_time',
                                              'Diff': 'total_gap',
                                              'Espaço': 'gap',
                                              'uid': 'uid'})
        db.insert_try_results(data)
        db.update_track_in_racing_tries(row_id, track)
        db.set_params_as_fetched('bronze_racing_tries', row_id)


if __name__ == '__main__':
    main('2022-02-01', '2022-06-30')
