import readers
import requests
import pandas as pd
import os

import random
from random import randrange
from random import choice
from string import ascii_lowercase



__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def get_maturities():
    return [1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5,
            6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 10, 20, 25, 25, 25, 25, 30, 30]


def get_tickers():
    path = os.path.join(__location__, 'tickers.csv')
    df = pd.read_csv(path, sep=',')
    return df.set_index('Symbol').to_dict()['Company']


def get_currencies():
    path = os.path.join(__location__, 'ccy.csv')
    df = pd.read_csv(path, sep=',')
    return df


def get_products():
    path = os.path.join(__location__, 'products.csv')
    df = pd.read_csv(path, sep=',')
    return df


def get_instruments():
    print(get_products().values.tolist())
    print(get_currencies().values.tolist())
    print(get_tickers())

    products = get_products().values
    currencies = get_currencies().values
    ticker_map = get_tickers()
    tickers = list(ticker_map.keys())

    rows = []
    for i in range(0, len(products)):
        product_id = i
        product = products[i]
        asset_class = product[0]
        sector = product[1]
        industry = product[2]
        product_type = product[3]
        currency = random.choice(currencies),
        maturity = random.choice(get_maturities())
        isin = "".join(choice(ascii_lowercase) for i in range(12))
        symbol = random.choice(tickers)
        issuer = ticker_map[symbol]
        rows.append({'ProductId': product_id,
               'AssetClass': asset_class,
               'Sector': sector,
               'Industry': industry,
               'ProductType': product_type,
               'Currency': currency,
               'Maturity': maturity,
               'Isin': isin,
               'Symbol': symbol,
               'Issuer': issuer
                     },)
    return pd.DataFrame(rows)


print(get_instruments())

