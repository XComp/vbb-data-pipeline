from datetime import datetime
from airflow import DAG
from airflow.operators import UnzipOperator, GZipOperator

dag = DAG('gzip_compression', description='Simple way of transforming the ZIP archive into a GZip folder structure.',
          schedule_interval=None,
          start_date=datetime(2017, 3, 20), catchup=False)

unzip_operator = UnzipOperator(task_id="unzip_task",
                               dag=dag,
                               source_archive="/tmp/VBB-GTFS.zip",
                               target_folder="/tmp/vbb-foobar/")

gzip_operator = GZipOperator(task_id="gzip_task", dag=dag)

unzip_operator >> gzip_operator

if __name__ == "__main__":
    dag.cli()
