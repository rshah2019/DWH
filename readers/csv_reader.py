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


def get_df(file_name):
    path = os.path.join(__location__, file_name)
    df = pd.read_csv(path, sep=',')
    return df


def get_books():
    df = get_df('books.csv')
    df.insert(0, 'BookId', df.index)
    return df


def get_brokers():
    df = get_df('brokers.csv')
    df.insert(0, 'PartyId', df.index)
    return df


def get_tickers():
    df = get_df('tickers.csv')
    return df.set_index('Symbol').to_dict()['Company']


def get_instruments():
    print(get_df('products.csv').values.tolist())
    print(get_df('ccy.csv').values.tolist())
    print(get_tickers())

    products = get_df('products.csv').values
    currencies = get_df('ccy.csv').values
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
        currency = random.choice(currencies)[0]
        maturity = random.choice(get_maturities())
        isin = "".join(choice(ascii_lowercase) for i in range(12)).upper()
        symbol = random.choice(tickers)
        issuer = ticker_map[symbol]

        if asset_class == 'Equity':
            maturity = None

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
    df = pd.DataFrame(rows)
    df['AssetClass'] = df['AssetClass'].astype(str)
    df['Sector'] = df['Sector'].astype(str)
    df['Industry'] = df['Industry'].astype(str)
    df['ProductType'] = df['ProductType'].astype(str)
    df['Currency'] = df['Currency'].astype(str)

    return df



