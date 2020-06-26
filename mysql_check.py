import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='PSNYD-KAFKA-03',
                                               database='Portfolio',
                                               user='bill',
                                               password='passpass')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

        import pandas as pd
        # initialize list of lists
        data = [-40, -50, -30, -10, -20, 10, 20, 30, 40, 50]
        df = pd.DataFrame(data, columns=['MV'])
        df.to_sql(con=connection, name='MV', if_exists='replace')

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")