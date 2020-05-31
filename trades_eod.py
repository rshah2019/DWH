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
    scale = 1
    if i % 988 == 0:
        scale = 25

    cur_row = {'PositionId': i,
               'ProductId': randrange(product_len),
               'Quantity': random.randint(-100000, 100000)*scale,
               'Exposure': random.randint(-100000, 100000)*scale}
    rows.append(cur_row)

df = pd.DataFrame(rows)

print(df)

directory = os.environ.get('directory', ".")

table = pa.Table.from_pandas(df)
pq.write_table(table, os.path.join(directory, 'trades_eod.parquet'))


print(df)


