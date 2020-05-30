import csv
import requests


TICKER_URL = 'https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/tickers.csv'


def get_tickers():
    ticker_map = {}
    with requests.Session() as s:
        download = s.get(TICKER_URL)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)[1:]
        for l in my_list:
            print(l)
            ticker_map[l[1]] = l[0]
    return ticker_map
