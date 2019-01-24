from datetime import datetime
import re
from typing import Callable

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import DagRunInitOperator, ExtractURLOperator, DownloadOperator, ChecksumOperator, \
    UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2019, 1, 18),
    "schedule_interval": None,
    "catchup": False,
    "owner": "mapohl",
    "base_folder": "/usr/local/data"
}


def extract_vbb_download_url(**kwargs) -> str:
    url = kwargs["url"]
    response = kwargs["response"]

    match = re.search(
        r'<a href="(/media/download/[0-9]*)" title="GTFS-Paket [^\"]*" class="teaser-link[ ]+m-download">',
        response.content.decode("utf-8"))

    if not match:
        return

    return "{}://{}{}".format(url.scheme, url.netloc, match.group(1))


def extract_vrs_download_url(**kwargs) -> str:
    response = kwargs["response"]
    
    match = re.search(
        r'<a href="(http://[^"]*.zip)" target="_blank" class="external-link-new-window">GTFS-Daten ohne SPNV-Daten</a>',
        response.content.decode("utf-8"))

    if not match:
        return None

    return match.group(1)


def create_provider_dag(
        parent_dag_id: str,
        provider_id: str,
        provider_description: str,
        provider_url: str,
        extract_func: Callable,
        check_url: bool,
        def_args: dict):
    sub_dag_id = "{}.{}".format(parent_dag_id, provider_id)

    sub_dag = DAG(dag_id=sub_dag_id,
                  description="This DAG extracts the GTFS archive provided by {}.".format(provider_description),
                  default_args=def_args)

    dagrun_init_operator = DagRunInitOperator(dag=sub_dag,
                                              task_id="init_dagrun_task")

    extract_url_operator = ExtractURLOperator(dag=sub_dag,
                                              task_id="extract_url_task",
                                              url=provider_url,
                                              extract_download_url=extract_func,
                                              check_url=check_url)

    download_operator = DownloadOperator(dag=sub_dag,
                                         task_id="download_task")

    checksum_operator = ChecksumOperator(dag=sub_dag,
                                         task_id="checksum_task")

    unzip_operator = UnzipOperator(dag=sub_dag,
                                   task_id="unzip_task")

    gzip_operator = GZipOperator(dag=sub_dag,
                                 task_id="gzip_task")

    dagrun_init_operator >> extract_url_operator >> download_operator >> checksum_operator >> unzip_operator \
        >> gzip_operator

    return sub_dag


dag_metadata = [
    ("vbb", "VBB Berlin/Brandenburg",
     "http://www.vbb.de/unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze",
     extract_vbb_download_url,
     True),
    ("vrs", "VRS Köln",
     "https://www.vrsinfo.de/fahrplan/oepnv-daten-fuer-webentwickler.html",
     extract_vrs_download_url,
     False)
]

main_dag_id = "gtfs_pipeline"
with DAG(dag_id=main_dag_id,
         description="Extracts the GTFS data from various sources.",
         default_args=default_args) as dag:
    start_task = DummyOperator(task_id="start")

    extract_tasks = []
    for prov_id, prov_desc, prov_url, prov_extract_func, prov_check_url, in dag_metadata:
        subdag = create_provider_dag(
                parent_dag_id=main_dag_id,
                provider_id=prov_id,
                provider_description=prov_desc,
                provider_url=prov_url,
                extract_func=prov_extract_func,
                check_url=prov_check_url,
                def_args=default_args)
        sub_dag_task = SubDagOperator(
            task_id=prov_id,
            dag=dag,
            subdag=subdag)
        extract_tasks.append(sub_dag_task)

    start_task >> extract_tasks
