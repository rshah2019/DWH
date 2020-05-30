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

df = pd.DataFrame(columns=['PositionId', 'Ticker', 'Quantity', 'Currency', 'Exposure'])
for i in range(int(max_row)):
    cur_row = [i]
    cur_row.append(random.choice(tickers))
    cur_row.append(randrange(100000))
    cur_row.append('USD')
    cur_row.append(randrange(100000))
    df.loc[i] = cur_row

dir = os.environ.get('directory', ".")

table = pa.Table.from_pandas(df)
pq.write_table(table, os.path.join(dir, 'trades_eod.parquet'))


print(df)


