import flask
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import os
import pandas as pd

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Have Fun</h1><p>Explore World of Data!!.</p>"


flask.Flask.sqlContext = None
flask.Flask.hdfs_address = None


def init():
    if app.sqlContext is None:
        spark = SparkSession.builder \
            .master('local') \
            .appName('myAppName') \
            .config('spark.executor.memory', '5gb') \
            .config("spark.cores.max", "6") \
            .getOrCreate()

        sc = spark.sparkContext

        # using SQLContext to read parquet file
        flask.Flask.sqlContext = SQLContext(sc)
        flask.Flask.hdfs_address = os.environ.get('hdfs_address')
    return flask.Flask.sqlContext

# A route to return all of the available entries in our catalog.
@app.route('/api/books', methods=['GET'])
def api_books():
    return get_request('books_eod.parquet')


@app.route('/api/counterparties', methods=['GET'])
def api_counterparties():
    return get_request('counterparties_eod.parquet')


@app.route('/api/instruments', methods=['GET'])
def api_instrument():
    return get_request('instruments_eod.parquet')


@app.route('/api/instruments_stats', methods=['GET'])
def api_instrument_stats():
    instruments = get_raw('instruments_eod.parquet')
    instruments.createOrReplaceTempView("instruments")
    s = init()
    res = s.sql("SELECT AssetClass, count(*)  from instruments group by AssetClass")
    return res.toPandas().to_html()


@app.route('/api/positions', methods=['GET'])
def api_positions():
    return get_request('positions_eod.parquet')


@app.route('/api/exposure_stats', methods=['GET'])
def api_positions_stats():
    df = get_df('positions_eod.parquet')
    df_2 = pd.DataFrame({'Stats': df['Exposure'].describe()})
    return df_2.to_html()


def get_raw(file_name):
    s = init()
    df = s.read.parquet(
        'hdfs://{}/user/root/05-29-2020/05-31-2020_17_18_UTC/{}'.format(flask.Flask.hdfs_address, file_name)).limit(1000)
    return df

def get_df(file_name):
    s = init()
    df = s.read.parquet(
        'hdfs://{}/user/root/05-29-2020/05-31-2020_17_18_UTC/{}'.format(flask.Flask.hdfs_address, file_name)).limit(1000)
    return df.toPandas()


def get_request(file_name):
    df = get_df(file_name)
    str = df.to_html()
    return str

app.run(host='0.0.0.0')