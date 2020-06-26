# Preparation
import numpy as np
import pandas as pd
import quandl

# Importing from Quandl API Bank of America Datas (.4 means close prices)
ticker = "WIKI/BAC.4"
df = quandl.get(ticker, start_date="2010-12-31", end_date="2018-12-31", collapse="daily")
percentage = df.pct_change()
percentage = percentage.dropna(how='any')

# Present the datas in ascending order
order_percentage = sorted(percentage["Close"])

print(order_percentage)

print("99.99% Actual loss won't exceed: ", "{0:.2f}%".format(np.percentile(order_percentage, .01) * 100))
print("99% Actual loss won't exceed: " + "{0:.2f}%".format(np.percentile(order_percentage, 1) * 100))
print("95% Actual loss won't exceed: " + "{0:.2f}%".format(np.percentile(order_percentage, 5) * 100))
print("Losses expected to exceed " + "{0:.2f}%".format(np.percentile(order_percentage, 5) * 100) + " " + str(
    .05 * len(percentage)) + " out of " + str(len(percentage)) + " days")
varg = np.percentile(order_percentage, 5)