from readers.csv_reader import *
import random
from random import randrange
import pandas as pd


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
