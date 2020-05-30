import csv
import requests
import pandas as pd
import os

TICKER_URL = 'https://raw.githubusercontent.com/plotly/dash-stock-tickers-demo-app/master/tickers.csv'

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def get_tickers_web():
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


def get_tickers():
    path = os.path.join(__location__, 'tickers.csv')
    df = pd.read_csv(path, sep=',')
    return df.set_index('Symbol').to_dict()['Company']