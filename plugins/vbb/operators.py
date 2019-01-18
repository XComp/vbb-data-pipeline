import zipfile
from os import listdir, makedirs
from os.path import isfile, join, exists
import gzip
import requests
from urllib.parse import urlparse, ParseResult

from typing import Callable
from airflow.models import BaseOperator, SkipMixin, TaskInstance
from airflow.utils.decorators import apply_defaults


class PipelineOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(PipelineOperator, self).__init__(*args, **kwargs)

        self.base_folder: str = base_folder

    def execute(self, context):
        task_folder = join(self.base_folder, self.dag_id, context["ds"], self.task_id)
        if not exists(task_folder):
            makedirs(task_folder)
            self.log.info("Created '{}'...".format(task_folder))

        return self._execute_with_folder(task_folder, context)

    def _execute_with_folder(self, task_folder: str, context):
        raise NotImplementedError()


class CheckURLOperator(SkipMixin, BaseOperator):

    @apply_defaults
    def __init__(self, url: str, extract_download_url: Callable, base_folder: str, *args, **kwargs):
        super(CheckURLOperator, self).__init__(*args, **kwargs)

        self.url: ParseResult = urlparse(url)
        self.base_folder = base_folder

        self.extract_download_url = extract_download_url

    def _get_dag_folder(self) -> str:
        return join(self.base_folder, self.dag_id)

    def _get_url_filepath(self) -> str:
        return join(self._get_dag_folder(), "url.txt")

    def _get_download_url(self):
        response = requests.get(self.url.geturl())
        download_url = self.extract_download_url(self.url, response)

        if not download_url:
            raise ValueError("No proper URL could have been extracted.")

        return download_url

    def _is_new_url(self, new_url: str):
        old_url = None
        if exists(self._get_url_filepath()):
            with open(self._get_url_filepath(), "r") as f:
                lines = f.readlines()
                assert len(lines) == 1

                old_url = lines[0]
        else:
            self.log.info("No previous URL discovered. Continue processing {}".format(new_url))
            # we have to make sure that the folder was created
            if not exists(self._get_dag_folder()):
                makedirs(self._get_dag_folder())
                self.log.info("No DAG folder didn't exist, yet. The folder was created now.")

        if old_url == new_url:
            # the URL didn't change - nothing to do
            self.log.info("No new URL discovered: {}".format(new_url))
            return False

        with open(self._get_url_filepath(), "w") as f:
            f.write(new_url)
            f.flush()

        return True

    def execute(self, context):
        download_url = self._get_download_url()

        if self._is_new_url(new_url=download_url):
            self.log.info("New URL found: {}".format(download_url))
            return download_url

        # no further processing is needed: skip all downstream tasks
        self.log.info("No new URL detected: Skipping all downstream tasks.")

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        return None


class DownloadOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DownloadOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        response = requests.get(context["task_instance"].xcom_pull("check_url_task"))

        target_file = join(task_folder, "vbb-archive.zip")
        with open(target_file, "wb") as zip_archive:
            zip_archive.write(response.content)

        self.log.info("Download finished.")

        return target_file


class UnzipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance: TaskInstance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        self.log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=task_folder)
                self.log.info("'{}' was extracted.".format(member_name))

        return task_folder


class GZipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(GZipOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance: TaskInstance = context["task_instance"]
        folder: str = task_instance.xcom_pull("unzip_task")
        self.log.info("Folder retrieved from upstream task: {}".format(folder))

        for f in listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and file_path.endswith(".gz"):
                continue

            with open(file_path, "rb") as plain_file:
                with gzip.open("{}.gz".format(join(task_folder, f)), "wb") as gzip_file:
                    gzip_file.writelines(plain_file.readlines())

            self.log.debug("{} was gzipped.".format(file_path))
