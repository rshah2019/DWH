import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
import os

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                    'two': ['foo', 'bar', 'baz'],
                    'three': [True, False, True]})

df = df.append(df, ignore_index=True)
print(df)