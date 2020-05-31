from readers.csv_reader import *
import random
from random import randrange
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


max_row = os.environ.get('max_row', '100')

product_len = len(get_products())

rows = []
for i in range(int(max_row)):
    cur_row = {'PositionId': i,
               'ProductId': randrange(product_len),
               'Quantity': randrange(100000),
               'Exposure': randrange(100000)}
    rows.append(cur_row)

df = pd.DataFrame(rows)

print(df)

directory = os.environ.get('directory', ".")

table = pa.Table.from_pandas(df)
pq.write_table(table, os.path.join(directory, 'trades_eod.parquet'))


print(df)


