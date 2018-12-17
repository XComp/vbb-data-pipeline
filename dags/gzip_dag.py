from datetime import datetime
from airflow import DAG
from airflow.operators import DownloadOperator, UnzipOperator, GZipOperator

from os.path import join

default_args = {
    "start_date": datetime(2018, 12, 12),
    "owner": "mapohl"
}

target_base_folder = "/data/vbb"

dag = DAG(dag_id='gzip_compression',
          description='Simple way of transforming the ZIP archive into a GZip folder structure.',
          schedule_interval=None,
          start_date=datetime(2018, 11, 15),
          catchup=False,
          default_args=default_args)

download_operator = DownloadOperator(task_id="download_task",
                                     dag=dag,
                                     target_folder=join(target_base_folder, "download_task"),
                                     uri="https://www.vbb.de/media/download/2029"
)

unzip_operator = UnzipOperator(task_id="unzip_task",
                               dag=dag,
                               target_folder=join(target_base_folder, "unzip_task"))

gzip_operator = GZipOperator(task_id="gzip_task",
                             dag=dag,
                             target_folder=join(target_base_folder, "gzip_task"))

download_operator >> unzip_operator >> gzip_operator

if __name__ == "__main__":
    dag.cli()
