import os

from time import time

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq


def ingest_fhv(user, password, host, port, db, table_name, csv_file, execution_date):
    
    print(table_name, csv_file, execution_date)
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    head_of_table = pd.read_parquet(csv_file)
    pq_file = pq.ParquetFile(csv_file)
    chunk_size = 100000
    total_rows = 0

    head_of_table.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    print('connection established successfully, inserting data...')

    t_start = time()

    for batch in pq_file.iter_batches(batch_size=chunk_size):
        df_chunk = batch.to_pandas()

        df_chunk.pickup_datetime = pd.to_datetime(df_chunk.pickup_datetime)
        df_chunk.dropOff_datetime = pd.to_datetime(df_chunk.dropOff_datetime)

        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

        total_rows += len(df_chunk)
    
    t_end = time()

    print(f'Inserted {total_rows} rows, took {t_end - t_start:.3f} seconds')
    print("Completed")
