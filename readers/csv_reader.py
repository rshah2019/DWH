import readers
import requests
import pandas as pd
import os


__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def get_tickers():
    path = os.path.join(__location__, 'tickers.csv')
    df = pd.read_csv(path, sep=',')
    return df.set_index('Symbol').to_dict()['Company']