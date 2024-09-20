from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import pytz

@dag (
    default_args = { "owner": "Camelia"},
    start_date = datetime(2024, 9, 5, tzinfo=pytz.timezone("Asia/Jakarta")),
    schedule_interval = "@daily",
    catchup = False,
    owner_links = {"Camelia":"mailto:cfitrianty@gmail.com"} 
    )

def assignment_trigger_dag():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

# trigger dag db extract - load    
    trigger_assignment_db_etl = TriggerDagRunOperator(
            task_id = 'trigger_assignment_db_etl',
            trigger_dag_id = 'assignment_db_etl',
            wait_for_completion = True
            )

# sensor menunggu assignment_db_etl selesai
    wait_for_el_completion = TimeDeltaSensor(
        task_id       = "wait_for_el_completion",
        delta         = timedelta(minutes=5),
        poke_interval = 30 ) 
    
# trigger dag aggregat report - email   
    trigger_assignment_email_report = TriggerDagRunOperator(
            task_id = 'trigger_assignment_email_report',
            trigger_dag_id = 'assignment_email_report' )

    start_task >> trigger_assignment_db_etl >> wait_for_el_completion >> trigger_assignment_email_report >> end_task

assignment_trigger_dag()


