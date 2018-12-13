import logging
import zipfile
from os import listdir, remove
from os.path import isfile, join
import gzip

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class UnzipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, source_archive, target_folder, *args, **kwargs):
        self.source_archive = source_archive
        self.target_folder = target_folder
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        with zipfile.ZipFile(self.source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=self.target_folder)

        return self.target_folder


class GZipOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(GZipOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        import pdb
        pdb.set_trace()

        task_instance = context["task_instance"]
        folder: str = task_instance.xcom_pull("unzip_task")
        log.info("Folder retrieved from upstream task: {}".format(folder))

        for f in listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and file_path.endswith(".gz"):
                continue

            with open(file_path, "rb") as plain_file:
                with gzip.open("{}.gz".format(file_path), "wb") as gzip_file:
                    gzip_file.writelines(plain_file.readlines())

            # remove file after GZipping its content into another file
            remove(file_path)
            log.debug("{} was gzipped.".format(file_path))


class GZipPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [UnzipOperator, GZipOperator]
