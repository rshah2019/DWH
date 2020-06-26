# Import pandas library
import pandas as pd
from analyze import *

# initialize list of lists
data = [-40, -50, -30, -10, -20, 10, 20, 30, 40, 50]

# Create the pandas DataFrame
df = pd.DataFrame(data, columns=['MV'])

neg = df[df < 0]

x = var_historic(neg)
y = var_historic(df)

print(x)
print(y)
