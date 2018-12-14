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
    def __init__(self, uri: str, target_folder: str, *args, **kwargs):
        super(DownloadOperator, self).__init__(*args, **kwargs)

        self.uri = uri

        # TODO: make this part generic
        #  i.e. create a parent class or look for a proper Airflow configuration
        self.target_folder: str = target_folder
        if not exists(self.target_folder):
            makedirs(self.target_folder)

    def execute(self, context):
        file_name = self.uri.split('/')[-1]
        target_file = join(self.target_folder, file_name)
        urlretrieve(self.uri, target_file)
        log.info("Download finished.")

        return target_file


class UnzipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_folder: str, *args, **kwargs):
        super(UnzipOperator, self).__init__(*args, **kwargs)

        # TODO: make this part generic
        #  i.e. create a parent class or look for a proper Airflow configuration
        self.target_folder: str = target_folder
        if not exists(self.target_folder):
            makedirs(self.target_folder)

    def execute(self, context):
        task_instance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=self.target_folder)

        return self.target_folder


class GZipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, target_folder: str, *args, **kwargs):
        super(GZipOperator, self).__init__(*args, **kwargs)

        # TODO: make this part generic
        #  i.e. create a parent class or look for a proper Airflow configuration
        self.target_folder: str = target_folder
        if not exists(self.target_folder):
            makedirs(self.target_folder)

    def execute(self, context):
        task_instance = context["task_instance"]
        folder: str = task_instance.xcom_pull("unzip_task")
        log.info("Folder retrieved from upstream task: {}".format(folder))

        for f in listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and file_path.endswith(".gz"):
                continue

            with open(file_path, "rb") as plain_file:
                with gzip.open("{}.gz".format(join(self.target_folder, f)), "wb") as gzip_file:
                    gzip_file.writelines(plain_file.readlines())

            log.debug("{} was gzipped.".format(file_path))


class GZipPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [DownloadOperator, UnzipOperator, GZipOperator]
