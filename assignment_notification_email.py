from airflow.decorators import dag, task
from airflow.utils.context import Context
from airflow.utils.email import send_email
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pytz
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    description = "DAG kedua berisi script Hasil analisa dan kirim ke email di schedul tiap jam 9 Pagi",
    default_args={"owner": "Camelia"},
    start_date=datetime(2024, 9, 5, 9, 0, 0, tzinfo=pytz.timezone("Asia/Jakarta")),
    schedule_interval = "0 9 * * *",
    catchup = False,
    tags = ["Assignment Day 4 - DAG ETL"],
    owner_links = {"Camelia":"mailto:cfitrianty@gmail.com"}
    )

def assignment_email_report():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def query_postgres(**kwargs):
        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()

        with postgres_hook.connect() as conn:
            ds = pd.read_sql(
                sql = "select * from dibimbing.data_siswa",
                con = conn,)
            ds['BMI'] = round(ds['WEIGHT'] / (ds['HEIGHT'] / 100.0) ** 2, 2)
            ds = ds[['NAME', 'GENDER', 'BMI']]
    
    # Ubah dataset ke format HTML
        htmltable = ds.to_html(index=False)

    # ambil data dari kwargs
        ti = kwargs['ti']

    # Kirim hasil ke email
        send_email (
            to="cfitrianty@gmail.com",
            subject="Report BMI Siswa",
            html_content=f"""
            <b>DAG ID</b>  : <i>{ti.dag_id}</i><br>
            <b>Task ID</b> : <i>{ti.task_id}</i><br>
            <b>Execution Date</b>: <i>{ti.execution_date}</i><br>
            <h3>Berikut terlampir laporan terbaru BMI Siswa:</h3>
            {htmltable}
                """ )
    
    start_task >> query_postgres() >> end_task

assignment_email_report()


