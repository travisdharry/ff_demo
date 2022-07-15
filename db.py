# Import dependencies
import os
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# Find environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", None)
# sqlalchemy deprecated urls which begin with "postgres://"; now it needs to start with "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# query the database, return a dataframe
def get_df(df):
    connection = False
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        query = f'SELECT * FROM {df}'
        cursor.execute(query)
        result = pd.read_sql(query, conn)
        return result
    except (Exception, Error) as error:
        print(error)
    finally:
        if connection:
            cursor.close()
            connection.close()
