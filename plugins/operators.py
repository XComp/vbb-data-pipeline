import logging
import zipfile
import re
from os import listdir, makedirs
from os.path import isfile, join, exists
import gzip
from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.operators.http_operator import HttpHook
from airflow.operators.sensors import HttpSensor
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class PipelineOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(PipelineOperator, self).__init__(*args, **kwargs)

        self.base_folder: str = base_folder

    def execute(self, context):
        task_folder = join(self.base_folder, context["dag"].dag_id, context["ds"], context["task_instance"].task_id)
        if not exists(task_folder):
            makedirs(task_folder)
            log.info("Created '{}'...".format(task_folder))

        return self._execute_with_folder(task_folder, context)

    def _execute_with_folder(self, task_folder: str, context):
        raise NotImplementedError()


class CheckURISensor(HttpSensor):

    @apply_defaults
    def __init__(self, uri: str, base_folder: str, http_conn_id: str, *args, **kwargs):
        super(HttpSensor, self).__init__(
            endpoint=uri,
            method="GET",
            response_check=self._response_check,
            http_conn_id=http_conn_id,
            *args,
            **kwargs)

        self.uri = urlparse(uri)
        self.url_file: str = join(base_folder, kwargs["dag"].dag_id, "url.txt")

    def _response_check(self, response):
        match = re.search(
            r'<a href="(/media/download/[0-9]*)" title="GTFS-Paket [^\"]*" class="teaser-link  m-download">',
            response.content.decode("utf-8"))
        if not match:
            log.error("The response could not be parsed.")
            return False

        new_path = match.group(1)
        old_path = self._read_from_url_file()

        if new_path == old_path:
            # the URL didn't change - nothing to do
            log.info("No new URL discovered: {}".format(new_path))
            return False

        log.info("No previous URL discovered. Continue processing {}".format(new_path))

        with open(self.url_file, "w") as f:
            f.write(new_path)
            f.flush()

        return True

    def _read_from_url_file(self):
        if not exists(self.url_file):
            return None

        with open(self.url_file, "r") as f:
            lines = f.readlines()
            assert len(lines) == 1

            return lines[0]

        return None

    def poke(self, context):
        return_value = super(CheckURISensor, self).poke(context)

        new_path = self._read_from_url_file()
        if new_path:
            context["task_instance"].xcom_push("url", new_path)

        return return_value


class DownloadOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, http_conn_id: str, *args, **kwargs):
        super(DownloadOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

        self.base_folder: str = base_folder
        self.http_conn_id = http_conn_id

    def _execute_with_folder(self, task_folder: str, context):
        http_hook = HttpHook("GET", http_conn_id=self.http_conn_id)
        response = http_hook.run(context["task_instance"].xcom_pull("check_url_task", key="url"))

        target_file = join(task_folder, "vbb-archive.zip")
        with open(target_file, "wb") as zip_archive:
            zip_archive.write(response.content)

        log.info("Download finished.")

        return target_file


class UnzipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(UnzipOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=task_folder)
                log.info("'{}' was extracted.".format(member_name))

        return task_folder


class GZipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(GZipOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance = context["task_instance"]
        folder: str = task_instance.xcom_pull("unzip_task")
        log.info("Folder retrieved from upstream task: {}".format(folder))

        for f in listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and file_path.endswith(".gz"):
                continue

            with open(file_path, "rb") as plain_file:
                with gzip.open("{}.gz".format(join(task_folder, f)), "wb") as gzip_file:
                    gzip_file.writelines(plain_file.readlines())

            log.debug("{} was gzipped.".format(file_path))


class GZipPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [CheckURISensor, DownloadOperator, UnzipOperator, GZipOperator]
