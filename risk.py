from readers.csv_reader import *
import random
from random import randrange
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def get_risk(max_row, prefix):
    print("generating risk for " + prefix)
    risk_df = None
    rows = []
    for i in range(int(max_row)):
        scale = 1
        if i % 988 == 0:
            scale = 2500

        cur_row = {"PositionId": i}
        for j in range(100):
            cur_row[prefix+str(j)+'Pct'] = random.randint(-1000000, 1000000) * scale

        rows.append(cur_row)

        if i % 100000 == 0:
            risk_df = risk_df.append(pd.DataFrame(rows), ignore_index=True) if risk_df is not None else pd.DataFrame(rows)
            rows = []
            print('i is ' + str(i))

    risk_df = risk_df.append(pd.DataFrame(rows), ignore_index=True) if risk_df is not None else pd.DataFrame(rows)
    return risk_df


def write_credit_risk(max_row, directory):
    credit_risk_df = get_risk(max_row, "Credit")
    print(credit_risk_df)
    credit_risk = pa.Table.from_pandas(credit_risk_df)
    pq.write_table(credit_risk, os.path.join(directory, 'credit_risk_eod.parquet'))


def write_rate_risk(max_row, directory):
    rate_risk_df = get_risk(max_row, "InterestRate")
    print(rate_risk_df)
    rate_risk = pa.Table.from_pandas(rate_risk_df)
    pq.write_table(rate_risk, os.path.join(directory, 'rate_risk_eod.parquet'))


def write_credit_risk(max_row, directory):
    credit_risk_df = get_risk(max_row, "Credit")
    print(credit_risk_df)
    credit_risk = pa.Table.from_pandas(credit_risk_df)
    pq.write_table(credit_risk, os.path.join(directory, 'credit_risk_eod.parquet'))


def write_vol_risk(max_row, directory):
    vol_risk_df = get_risk(max_row, "Volatility")
    print(vol_risk_df)
    volatility_risk = pa.Table.from_pandas(vol_risk_df)
    pq.write_table(volatility_risk, os.path.join(directory, 'volatility_risk_eod.parquet'))
