from readers.csv_reader import *
import random
from random import randrange
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


max_row = os.environ.get('max_row', '100')
ins_df = get_instruments()
product_len = len(ins_df)

pos_df = None
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





