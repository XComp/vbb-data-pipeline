import zipfile
import re
from os import listdir, makedirs
from os.path import isfile, join, exists
import gzip

from airflow.models import BaseOperator, SkipMixin, LoggingMixin
from airflow.operators.http_operator import HttpHook
from airflow.plugins_manager import AirflowPlugin
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


class CheckURIOperator(SkipMixin, BaseOperator):

    @apply_defaults
    def __init__(self, uri: str, base_folder: str, http_conn_id: str, *args, **kwargs):
        super(CheckURIOperator, self).__init__(*args, **kwargs)

        self.parent_uri = uri
        self.http_conn_id = http_conn_id
        self.uri_filepath = join(base_folder, self.dag_id, "url.txt")

    def _get_download_uri(self):
        http_hook = HttpHook("GET", http_conn_id=self.http_conn_id)
        response = http_hook.run(self.parent_uri)

        match = re.search(
            r'<a href="(/media/download/[0-9]*)" title="GTFS-Paket [^\"]*" class="teaser-link[ ]+m-download">',
            response.content.decode("utf-8"))
        if not match:
            self.log.error("The response could not be parsed.")
            return False

        return match.group(1)

    def _is_new_uri(self, new_uri: str):
        old_uri = None
        if exists(self.uri_filepath):
            with open(self.uri_filepath, "r") as f:
                lines = f.readlines()
                assert len(lines) == 1

                old_uri = lines[0]
        else:
            self.log.info("No previous URL discovered. Continue processing {}".format(new_uri))

        if old_uri == new_uri:
            # the URL didn't change - nothing to do
            self.log.info("No new URL discovered: {}".format(new_uri))
            return False

        with open(self.uri_filepath, "w") as f:
            f.write(new_uri)
            f.flush()

        return True

    def execute(self, context):
        download_uri = self._get_download_uri()

        if self._is_new_uri(new_uri=download_uri):
            self.log.info("New URI found: {}".format(download_uri))
            return download_uri

        # no further processing is needed: skip all downstream tasks
        self.log.info("No new URI detected: Skipping all downstream tasks.")

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        return None


class DownloadOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, http_conn_id: str, *args, **kwargs):
        super(DownloadOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

        self.base_folder: str = base_folder
        self.http_conn_id = http_conn_id

    def _execute_with_folder(self, task_folder: str, context):
        http_hook = HttpHook("GET", http_conn_id=self.http_conn_id)
        response = http_hook.run(context["task_instance"].xcom_pull("check_url_task"))

        target_file = join(task_folder, "vbb-archive.zip")
        with open(target_file, "wb") as zip_archive:
            zip_archive.write(response.content)

        self.log.info("Download finished.")

        return target_file


class UnzipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(UnzipOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        self.log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=task_folder)
                self.log.info("'{}' was extracted.".format(member_name))

        return task_folder


class GZipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(GZipOperator, self).__init__(base_folder=base_folder, *args, **kwargs)

    def _execute_with_folder(self, task_folder: str, context):
        task_instance = context["task_instance"]
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
