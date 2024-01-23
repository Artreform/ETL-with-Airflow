# ETL-with-Airflow
This is my homework for Week 2 of "DE Zoomcamp course".

Link to the course: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2022/week_2_data_ingestion.



The idea was:
Ingest data from the Web -->> transforming data in Pandas -->> load data in local database



The whole proceess consists of several steps:

1. Setting up working enviroment in Docker container: Airflow, Postgres, PgAdmin;
2. Data ingestion about NY taxi rides 2023 from the web: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page;
3. Transforming data in chunks (one table may consist of > 1 mln.rows) in Postgres.

All process was written in Python and orchestrated throw DAGs in Airflow.

