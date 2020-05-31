from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import os

#os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-11"
#os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
#os.environ["hadoop.home.dir"] = "C:/Users/rvs"

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

df = sqlContext.read.parquet('hdfs://PSNYD-KAFKA-01:9000/user/RVS/example.parquet')
print(df)

sc.sto

# to read parquet file
#df = sqlContext.read.parquet('hdfs://PSNYD-KAFKA-01:9000/user/RVS/example.parquet')
#print(df)
