import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
import os

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                    'two': ['foo', 'bar', 'baz'],
                    'three': [True, False, True]},
                  index=list('abc'))

table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')

table2 = pq.read_table('example.parquet')
pd2 = table2.to_pandas()
print(pd2)


df = pd.read_csv("C:\\Temp\\100 Sales Records.readers")
pdd = df.to_parquet('output.parquet')

client_hdfs = InsecureClient('http://PSNYD-KAFKA-01:9870')
#client_hdfs = InsecureClient('http://PSNYP-AFLOW-02:50070')
f = open('output.parquet', errors='ignore')
#client_hdfs.write('output2.parquet', f, encoding='utf-8')


with client_hdfs.read('output2.parquet', encoding='utf-8') as reader:
    content = reader.read()
    print(content)

df = client_hdfs.list("/user/RVS")
print(df)

#fs = pa.hdfs.connect('PSNYP-AFLOW-02', 9870, 'hdfs')
#pq.write_to_dataset(table, root_path='dataset_name', partition_cols=['one', 'two'], filesystem=fs)


