import flask
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import os


app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"


def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))

# A route to return all of the available entries in our catalog.
@app.route('/api/books', methods=['GET'])
def api_all():
    spark = SparkSession.builder \
        .master('local') \
        .appName('myAppName') \
        .config('spark.executor.memory', '5gb') \
        .config("spark.cores.max", "6") \
        .getOrCreate()

    sc = spark.sparkContext

    # using SQLContext to read parquet file
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(
        'hdfs://PSNYD-KAFKA-01:9000/user/root/05-29-2020/05-31-2020_17_18_UTC/books_eod.parquet')
    df.show()

    str = getShowString(df)

    spark.stop()

    return str

app.run(host='0.0.0.0')