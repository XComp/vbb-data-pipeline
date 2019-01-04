from datetime import datetime
from airflow import DAG
from airflow.operators import Copy2DatabaseOperator, CheckSchemaOperator, CreateSchemaOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

base_folder = "/usr/local/data"
task_folder_name = "unzip_task"

db_schema = "vbb"
db_conn_id = "postgres_vbb"

dag = DAG(dag_id="vbb_database",
          description="This DAG loads the available data into a database.",
          schedule_interval="0 0 * * 0",
          catchup=False,
          default_args=default_args)

check_schema_operator = CheckSchemaOperator(task_id="check_schema_task",
                                            dag=dag,
                                            create_schema_task_id="create_schema_task",
                                            load_data_task_id="copy2db_task",
                                            postgres_conn_id=db_conn_id,
                                            schema=db_schema)

create_schema_operator = CreateSchemaOperator(task_id="create_schema_task",
                                              dag=dag,
                                              postgres_conn_id=db_conn_id,
                                              schema=db_schema)

copy2db_operator = Copy2DatabaseOperator(task_id="copy2db_task",
                                         dag=dag,
                                         base_folder=base_folder,
                                         task_folder_name=task_folder_name,
                                         postgres_conn_id=db_conn_id,
                                         schema=db_schema)

check_schema_operator >> create_schema_operator >> copy2db_operator
check_schema_operator >> copy2db_operator

if __name__ == "__main__":
    dag.cli()
