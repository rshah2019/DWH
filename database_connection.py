import sqlalchemy
import os


def get_connection():
    database_username = os.environ.get('mysql_user')
    database_password = os.environ.get('mysql_pass')
    database_ip = os.environ.get('mysql_ip')
    database_name = 'Portfolio'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))
    return database_connection
