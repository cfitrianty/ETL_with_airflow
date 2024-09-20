from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pandas as pd
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pytz

@dag(
    description = "DAG pertama berisi script ETL dan Schedul tiap jam 7 Pagi",
    start_date=datetime(2024, 9, 5, 7, 0, 0, tzinfo=pytz.timezone("Asia/Jakarta")),
    schedule_interval = "0 7 * * *",
    catchup = False,
    tags = ["Assignment Day 4 - DAG ETL"],
    default_args = { "owner": "Camelia"},
    owner_links = {"Camelia":"mailto:cfitrianty@gmail.com"})

def assignment_db_etl():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

## Extract data dari mysql 
    @task
    def extract_from_source():
        mysql_hook = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()
        with mysql_hook.connect() as conn:
            df = pd.read_sql(
                sql = "SELECT * FROM dibimbing.data_siswa",
                con = conn,
            )
            df['LAST_UPDATED'] = datetime.now(tz=pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")

        df.to_parquet("data/data_siswa.parquet", index=False)
        print("data berhasil diextract")

## Load data ke Postgres
    @task
    def load_to_target():
        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
        df            = pd.read_parquet("data/data_siswa.parquet")

        with postgres_hook.connect() as conn:
            df = df.to_sql(
                name      = "data_siswa",
                con       = conn,
                index     = False,
                schema    = "dibimbing",
                if_exists = "replace",
            )

        print("data berhasil diload")
    
    start_task >> extract_from_source() >> load_to_target() >> end_task

assignment_db_etl()




