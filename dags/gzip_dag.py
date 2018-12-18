from datetime import datetime
from airflow import DAG
from airflow.operators import DownloadOperator, UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

base_folder = "/usr/local/data"

dag = DAG(dag_id="vbb",
          description="This DAG extracts the data out of the VBB opendata access point for GTFS data and processes it.",
          schedule_interval="0 0 * * 0",
          start_date=datetime(2018, 11, 15),
          catchup=False,
          default_args=default_args)

download_operator = DownloadOperator(task_id="download_task",
                                     dag=dag,
                                     base_folder=base_folder,
                                     uri="https://www.vbb.de/media/download/2029")

unzip_operator = UnzipOperator(task_id="unzip_task",
                               dag=dag,
                               base_folder=base_folder)

gzip_operator = GZipOperator(task_id="gzip_task",
                             dag=dag,
                             base_folder=base_folder)

download_operator >> unzip_operator >> gzip_operator

if __name__ == "__main__":
    dag.cli()
