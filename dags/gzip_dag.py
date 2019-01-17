from datetime import datetime
from airflow import DAG
from airflow.operators import CheckURLOperator, DownloadOperator, UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl",
    "base_folder": "/usr/local/data"
}

vbb_url = "http://www.vbb.de/unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze"

with DAG(dag_id="vbb",
         description="This DAG extracts the data out of the VBB opendata access point for GTFS data and processes it.",
         schedule_interval="0 0 * * 0",
         start_date=datetime(2018, 11, 15),
         catchup=False,
         default_args=default_args) as dag:

    check_url_operator = CheckURLOperator(task_id="check_url_task", url=vbb_url)

    download_operator = DownloadOperator(task_id="download_task")

    unzip_operator = UnzipOperator(task_id="unzip_task")

    gzip_operator = GZipOperator(task_id="gzip_task")

    check_url_operator >> download_operator >> unzip_operator >> gzip_operator
