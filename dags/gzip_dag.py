from datetime import datetime
from airflow import DAG
from airflow.operators import CheckURISensor, DownloadOperator, UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

base_folder = "/usr/local/data"
http_conn_id = "vbb_default"

dag = DAG(dag_id="vbb",
          description="This DAG extracts the data out of the VBB opendata access point for GTFS data and processes it.",
          schedule_interval="0 0 * * 0",
          start_date=datetime(2018, 11, 15),
          catchup=False,
          default_args=default_args)

check_url_sensor = CheckURISensor(task_id="check_url_task",
                                  dag=dag,
                                  base_folder=base_folder,
                                  http_conn_id=http_conn_id,
                                  uri="unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze")

download_operator = DownloadOperator(task_id="download_task",
                                     dag=dag,
                                     base_folder=base_folder,
                                     http_conn_id=http_conn_id)

unzip_operator = UnzipOperator(task_id="unzip_task",
                               dag=dag,
                               base_folder=base_folder)

gzip_operator = GZipOperator(task_id="gzip_task",
                             dag=dag,
                             base_folder=base_folder)

check_url_sensor >> download_operator >> unzip_operator >> gzip_operator

if __name__ == "__main__":
    dag.cli()
