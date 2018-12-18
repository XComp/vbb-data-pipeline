import logging
import zipfile
from os import listdir, makedirs
from os.path import isfile, join, exists
import gzip
from urllib.request import urlretrieve

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class DownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, uri: str, base_folder: str, *args, **kwargs):
        super(DownloadOperator, self).__init__(*args, **kwargs)

        self.uri = uri
        self.base_folder: str = base_folder

    def execute(self, context):
        task_folder = join(self.base_folder, context["dag"].dag_id, context["ds"], context["task_instance"].task_id)
        if not exists(task_folder):
            makedirs(task_folder)
            log.info("Created '{}'...".format(task_folder))

        target_file = join(task_folder, "vbb-archive.zip")
        urlretrieve(self.uri, target_file)
        log.info("Download finished.")

        return target_file


class UnzipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(UnzipOperator, self).__init__(*args, **kwargs)

        self.base_folder: str = base_folder

    def execute(self, context):
        task_folder = join(self.base_folder, context["dag"].dag_id, context["ds"], context["task_instance"].task_id)
        if not exists(task_folder):
            makedirs(task_folder)
            log.info("Created '{}'...".format(task_folder))

        task_instance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=task_folder)

        return task_folder


class GZipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, base_folder: str, *args, **kwargs):
        super(GZipOperator, self).__init__(*args, **kwargs)

        self.base_folder: str = base_folder

    def execute(self, context):
        task_folder = join(self.base_folder, context["dag"].dag_id, context["ds"], context["task_instance"].task_id)
        if not exists(task_folder):
            makedirs(task_folder)
            log.info("Created '{}'...".format(task_folder))

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
    operators = [DownloadOperator, UnzipOperator, GZipOperator]
