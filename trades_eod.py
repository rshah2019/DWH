import pandas as pd
from numpy.random import randint
from csv_reader import get_tickers
import random
from random import randrange
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


max_row = os.environ.get('max_row', '100')
tickers = list(get_tickers().keys())
print(tickers)

rows = []
for i in range(int(max_row)):
    cur_row = {'PositionId': i,
               'Ticker': random.choice(tickers),
               'Quantity': randrange(100000),
               'Currency': 'USD',
               'Exposure': randrange(100000)}
    rows.append(cur_row)

df = pd.DataFrame(rows)

directory = os.environ.get('directory', ".")

table = pa.Table.from_pandas(df)
pq.write_table(table, os.path.join(directory, 'trades_eod.parquet'))


print(df)


