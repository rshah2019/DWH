import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import subprocess

"""
from pyarrow import fs




#t = ('C:\\hadoop\\hadoop-3.1.0\\bin\\hdfs', 'classpath', '--glob')

#subprocess.run(t)

#os.environ['HADOOP_HOME'] = ""
#os.environ['CLASSPATH'] = "$HADOOP_HOME/bin/hdfs classpath --glob"

if 'HADOOP_HOME' in os.environ:
    hadoop_bin = os.path.normpath(os.environ['HADOOP_HOME'])
    hadoop_bin = os.path.join(hadoop_bin, 'bin')
    #os.environ['HADOOP_HOME'] = hadoop_bin
else:
    hadoop_bin = 'hadoop'

os.chdir(hadoop_bin)
hadoop_bin_exe = os.path.join(hadoop_bin, 'hadoop.cmd')
classpath = subprocess.check_output([hadoop_bin_exe, 'classpath', '--glob'])

os.environ['CLASSPATH'] = classpath.decode('utf-8')

"""

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                    'two': ['foo', 'bar', 'baz'],
                    'three': [True, False, True]},
                  index=list('abc'))

fs = pa.hdfs.connect(host='PSNYD-KAFKA-01', port=9000)
table = pa.Table.from_pandas(df)
pq.write_table(table, 'first.parquet')



