from datetime import datetime
from airflow import DAG
from airflow.operators import CheckURIOperator, DownloadOperator, UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

base_folder = "/usr/local/data"
http_conn_id = "vbb_default"

with DAG(dag_id="vbb",
         description="This DAG extracts the data out of the VBB opendata access point for GTFS data and processes it.",
         schedule_interval="0 0 * * 0",
         start_date=datetime(2018, 11, 15),
         catchup=False,
         default_args=default_args) as dag:

    check_url_operator = CheckURIOperator(task_id="check_url_task",
                                          base_folder=base_folder,
                                          http_conn_id=http_conn_id,
                                          uri="unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze")

    download_operator = DownloadOperator(task_id="download_task",
                                         base_folder=base_folder,
                                         http_conn_id=http_conn_id)

    unzip_operator = UnzipOperator(task_id="unzip_task",
                                   base_folder=base_folder)

    gzip_operator = GZipOperator(task_id="gzip_task",
                                 base_folder=base_folder)

    check_url_operator >> download_operator >> unzip_operator >> gzip_operator
