from scraping import scrap

if __name__ == '__main__':
    kgv_data = scrap.KgvCollectData()
    kgv_data.date_filter('2019-06-01', '2019-06-02')
    kgv_data.gen_params_list()
    kgv_data.gen_results_urls()
    kgv_data.collect_results()
    kgv_data.save_results('../Data/kart_data.csv')
