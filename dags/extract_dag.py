from datetime import datetime
import re
from urllib.parse import ParseResult
from requests import Response
from typing import Callable

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import CheckURLOperator, DownloadOperator, UnzipOperator, GZipOperator

default_args = {
    "start_date": datetime(2019, 1, 18),
    "schedule_interval": None,
    "catchup": False,
    "owner": "mapohl",
    "base_folder": "/usr/local/data"
}


def extract_vbb_download_url(url: ParseResult, response: Response) -> str:
    match = re.search(
        r'<a href="(/media/download/[0-9]*)" title="GTFS-Paket [^\"]*" class="teaser-link[ ]+m-download">',
        response.content.decode("utf-8"))

    if not match:
        return

    return "{}://{}{}".format(url.scheme, url.netloc, match.group(1))


def extract_vrs_download_url(url: ParseResult, response: Response) -> str:
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
        def_args: dict):
    sub_dag_id = "{}.{}".format(parent_dag_id, provider_id)

    sub_dag = DAG(dag_id=sub_dag_id,
                  description="This DAG extracts the GTFS archive provided by {}.".format(provider_description),
                  default_args=def_args)

    check_url_operator = CheckURLOperator(dag=sub_dag,
                                          task_id="check_url_task",
                                          url=provider_url,
                                          extract_download_url=extract_func)

    download_operator = DownloadOperator(dag=sub_dag,
                                         task_id="download_task")

    unzip_operator = UnzipOperator(dag=sub_dag,
                                   task_id="unzip_task")

    gzip_operator = GZipOperator(dag=sub_dag,
                                 task_id="gzip_task")

    check_url_operator >> download_operator >> unzip_operator >> gzip_operator

    return sub_dag


dag_metadata = [
    ("vbb", "VBB Berlin/Brandenburg",
     "http://www.vbb.de/unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze",
     extract_vbb_download_url),
    ("vrs", "VRS Köln",
     "https://www.vrsinfo.de/fahrplan/oepnv-daten-fuer-webentwickler.html",
     extract_vrs_download_url)
]

main_dag_id = "gtfs_pipeline"
with DAG(dag_id=main_dag_id,
         description="Extracts the GTFS data from various sources.",
         default_args=default_args) as dag:
    start_task = DummyOperator(task_id="start")

    extract_tasks = []
    for prov_id, prov_desc, prov_url, prov_extract_func in dag_metadata:
        subdag = create_provider_dag(
                parent_dag_id=main_dag_id,
                provider_id=prov_id,
                provider_description=prov_desc,
                provider_url=prov_url,
                extract_func=prov_extract_func,
                def_args=default_args)
        sub_dag_task = SubDagOperator(
            task_id=prov_id,
            dag=dag,
            subdag=subdag)
        extract_tasks.append(sub_dag_task)

    start_task >> extract_tasks
