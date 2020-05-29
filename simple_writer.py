import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                    'two': ['foo', 'bar', 'baz'],
                    'three': [True, False, True]},
                  index=list('abc'))

dir = os.environ.get('directory', ".")

table = pa.Table.from_pandas(df)
pq.write_table(table, os.path.join(dir, 'simple_df.parquet'))

