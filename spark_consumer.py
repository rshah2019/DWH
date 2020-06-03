from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import os

# initialise sparkContext
# fetch test
spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
sqlContext = SQLContext(sc)

hdfs_adress = os.environ.get('hdfs_address')
df = sqlContext.read.parquet('hdfs://{}/user/root/05-29-2020/05-31-2020_17_18_UTC/books_eod.parquet'.format(hdfs_adress))
df.show()
