# Import dataframe into MySQL
import pandas as pd
import sqlalchemy


def get_connection():
    database_username = 'bill'
    database_password = 'passpass'
    database_ip       = 'PSNYD-KAFKA-03'
    database_name     = 'Portfolio'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password,
                                                        database_ip, database_name))
    return database_connection


# initialize list of lists
data = [-40, -50, -30, -10, -20, 10, 20, 30, 40, 50]
df = pd.DataFrame(data, columns=['MV'])
df.to_sql(con=get_connection(), name='MV', if_exists='replace')