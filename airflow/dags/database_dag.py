from datetime import datetime
from airflow import DAG
from airflow.operators import DataSelectOperator, NewDataIdentifyOperator, LoadNewDataOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

base_folder = "/usr/local/data"

db_schema = "gtfs"
db_conn_id = "postgres_gtfs"

with DAG(dag_id="database_load",
         description="This DAG loads the available data into a database.",
         schedule_interval=None,
         catchup=False,
         default_args=default_args) as dag:

    available_data_task = DataSelectOperator(task_id="data_select_task",
                                             base_folder=base_folder)

    new_data_task = NewDataIdentifyOperator(task_id="new_data_task",
                                            postgres_conn_id=db_conn_id,
                                            schema=db_schema)

    load_data_task = LoadNewDataOperator(task_id="load_data_task",
                                         base_folder=base_folder,
                                         postgres_conn_id=db_conn_id,
                                         schema=db_schema)

    available_data_task >> new_data_task >> load_data_task

if __name__ == "__main__":
    dag.cli()
