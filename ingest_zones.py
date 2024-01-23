import os

from time import time

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq


def ingest_zones(user, password, host, port, db, table_name, csv_file, execution_date):
    
    print(table_name, csv_file, execution_date)
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_csv(csv_file)
    
    print('connection established successfully, inserting data...')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    print("Completed")
