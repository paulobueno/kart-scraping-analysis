from scraping.scrap import KgvCollectData

if __name__ == '__main__':
    kgv_data = KgvCollectData(date_range=('2021-01-01', '2021-12-31'))
    kgv_data(to_csv='./Data/data.csv')

