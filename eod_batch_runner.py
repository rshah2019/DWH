from readers.csv_reader import *
import random
from random import randrange
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import datetime
from risk import *


def run_eod_batch():

    trade_date = datetime.datetime(2020, 5, 29)
    knowledge_time = datetime.datetime.now()

    max_row = os.environ.get('max_row', '100')
    ins_df = get_instruments()
    books_df = get_df('books.csv')
    cps_df = get_df('brokers.csv')

    product_len = len(ins_df)
    books_len = len(books_df)
    cps_len = len(cps_df)

    pos_df = None
    rows = []
    for i in range(int(max_row)):
        scale = 1
        if i % 988 == 0:
            scale = 25

        cur_row = {'PositionId': i,
                   'ProductId': randrange(product_len),
                   'BookId': randrange(books_len),
                   'CounterpartyId': randrange(cps_len),
                   'Quantity': random.randint(-100000, 100000)*scale,
                   'Exposure': random.randint(-100000, 100000)*scale,
                   'TradeDate': trade_date,
                   'KnowledgeTime': knowledge_time
                   }
        rows.append(cur_row)

        if i % 100000 == 0:
            pos_df = pos_df.append(pd.DataFrame(rows), ignore_index=True) if pos_df is not None else pd.DataFrame(rows)
            rows = []
            print('i is ' + str(i))

    pos_df = pos_df.append(pd.DataFrame(rows), ignore_index=True) if pos_df is not None else pd.DataFrame(rows)
    print(pos_df)

    directory = os.environ.get('directory', ".")
    positions = pa.Table.from_pandas(pos_df)
    pq.write_table(positions, os.path.join(directory, 'trades_eod.parquet'))

    print(ins_df)
    instruments = pa.Table.from_pandas(ins_df)
    pq.write_table(instruments, os.path.join(directory, 'instruments_eod.parquet'))

    print(books_df)
    books = pa.Table.from_pandas(books_df)
    pq.write_table(books, os.path.join(directory, 'books_eod.parquet'))

    print(cps_df)
    counterparties = pa.Table.from_pandas(cps_df)
    pq.write_table(counterparties, os.path.join(directory, 'counterparties_eod.parquet'))

    write_credit_risk(max_row, directory)
    write_rate_risk(max_row, directory)
    write_vol_risk(max_row, directory)


if __name__ == "__main__":
    run_eod_batch()




