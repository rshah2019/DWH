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

df = sqlContext.read.parquet('hdfs://PSNYD-KAFKA-01:9000/user/root/05-29-2020/05-31-2020_17_18_UTC/books_eod.parquet')
df.show()
