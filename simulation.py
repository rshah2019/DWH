from readers.csv_reader import *
import random
from random import randrange
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from analyze import *

tenors = ['0D', '1D', '5D', '10D', '1M', '2M', '3M', '4M', '5M', '6M', '1Y', '2Y', '5Y', '10Y']


def get_scale(i):
    scale = 1
    if i != 0:
        if i % 98 == 0:
            scale = 2500
        if i % 56 == 0:
            scale = -300
        if i % 67 == 0:
            scale = 9000
    return scale


def get_portfolio_simulation(prefix, paths):
    print("generating simulation for " + prefix)
    simulation_df = None
    rows = []
    for p in range(paths):
        scale = get_scale(p)
        cur_row = {'Path': p}
        for j in tenors:
            cur_row[j] = random.randint(-1000000, 1000000) * scale
        rows.append(cur_row)

    simulation_df = simulation_df.append(pd.DataFrame(rows), ignore_index=True) if simulation_df is not None else pd.DataFrame(rows)
    d = var_historic(simulation_df)
    return simulation_df


def get_position_simulation(max_row, prefix, paths):
    print("generating simulation for " + prefix)
    simulation_df = None
    rows = []
    for i in range(int(max_row)):
        scale = get_scale(i)
        for p in range(paths):
            cur_row = {'PositionId': i, 'Path': p}
            for j in tenors:
                cur_row[j] = random.randint(-1000000, 1000000) * scale
            rows.append(cur_row)

        if i % 100 == 0:
            simulation_df = simulation_df.append(pd.DataFrame(rows), ignore_index=True) if simulation_df is not None else pd.DataFrame(rows)
            rows = []
            print('i is ' + str(i))
    simulation_df = simulation_df.append(pd.DataFrame(rows), ignore_index=True) if simulation_df is not None else pd.DataFrame(rows)
    return simulation_df


def write_position_simulation(position_sims, position_paths, directory):
    position_simulation_df = get_position_simulation(position_sims, "Position", position_paths)
    print(position_simulation_df)
    position_simulation = pa.Table.from_pandas(position_simulation_df)
    pq.write_table(position_simulation, os.path.join(directory, 'position_simulation_eod.parquet'))


def write_portfolio_simulation(portfolio_paths, directory):
    portfolio_simulation_df = get_portfolio_simulation("Portfolio", portfolio_paths)
    print(portfolio_simulation_df)
    portfolio_simulation = pa.Table.from_pandas(portfolio_simulation_df)
    pq.write_table(portfolio_simulation, os.path.join(directory, 'portfolio_simulation_eod.parquet'))

