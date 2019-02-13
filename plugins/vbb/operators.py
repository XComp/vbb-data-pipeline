import zipfile
from os import listdir
from os.path import isfile, join, exists
from shutil import copyfile
import gzip
import requests
from urllib.parse import urlparse, ParseResult

from typing import Callable
from airflow.models import SkipMixin, TaskInstance
from airflow.utils.decorators import apply_defaults
from utils.operators import PipelineOperator


class ExtractURLOperator(SkipMixin, PipelineOperator):

    @apply_defaults
    def __init__(self, url: str, extract_download_url: Callable, check_url: bool, *args, **kwargs):
        super(ExtractURLOperator, self).__init__(create_task_folder=False, *args, **kwargs)

        self.url: ParseResult = urlparse(url)
        self.extract_download_url = extract_download_url
        self.check_url = check_url

    def _get_url_filepath(self) -> str:
        return join(self.get_dag_folder(), "url.txt")

    def _get_download_url(self):
        response = requests.get(self.url.geturl())
        download_url = self.extract_download_url(url=self.url, response=response)

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

        if old_url == new_url:
            # the URL didn't change - nothing to do
            self.log.info("No new URL discovered: {}".format(new_url))
            return False

        with open(self._get_url_filepath(), "w") as f:
            f.write(new_url)
            f.flush()

        return True

    def _execute_with_folder(self, context):
        download_url = self._get_download_url()

        if not self.check_url or self._is_new_url(new_url=download_url):
            self.log.info("New URL found: {}".format(download_url))
            return download_url

        # no further processing is needed: skip all downstream tasks
        self.log.info("No new URL detected: Skipping all downstream tasks.")

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        return None


class ChecksumOperator(SkipMixin, PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ChecksumOperator, self).__init__(create_task_folder=False, *args, **kwargs)

    @staticmethod
    def _calculate_checksum(zip_archive_path: str):
        z = zipfile.ZipFile(zip_archive_path, "r")
        checksum = 0
        for info in z.infolist():
            checksum = checksum ^ info.CRC

        return checksum

    def _get_checksum_file(self):
        return "{}/checksum.txt".format(self.get_dag_folder())

    def _get_old_checksum(self):
        if not exists(self._get_checksum_file()):
            return None

        with open(self._get_checksum_file(), "r") as f:
            line = f.readline().strip()

        if line:
            return int(line, 16)

        return None

    def _save_checksum(self, checksum: int):
        with open(self._get_checksum_file(), "w") as f:
            f.write(hex(checksum))

    def _execute_with_folder(self, context):
        archive_path = context["task_instance"].xcom_pull("download_task")

        checksum: int = ChecksumOperator._calculate_checksum(zip_archive_path=archive_path)
        old_checksum: int = self._get_old_checksum()

        if checksum != old_checksum:
            self._save_checksum(checksum=checksum)
            return archive_path

        # no further processing is needed: skip all downstream tasks
        self.log.info("Archive didn't change: Skipping all downstream tasks.")

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)


class DownloadOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DownloadOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, context):
        response = requests.get(context["task_instance"].xcom_pull("extract_url_task"))

        target_file = join(self.get_task_folder(), "archive.zip")
        with open(target_file, "wb") as zip_archive:
            zip_archive.write(response.content)

        self.log.info("Download finished.")

        return target_file


class FakeDownloadOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, source_file, *args, **kwargs):
        super(FakeDownloadOperator, self).__init__(*args, **kwargs)

        self.source_file = source_file

    def _execute_with_folder(self, context):
        target_file = join(self.get_task_folder(), "archive.zip")
        copyfile(self.source_file, target_file)

        self.log.info("Copying finished.")

        return target_file


class UnzipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(UnzipOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, context):
        task_instance: TaskInstance = context["task_instance"]
        source_archive: str = task_instance.xcom_pull("download_task")
        self.log.info("Source archive retrieved from upstream task: {}".format(source_archive))

        with zipfile.ZipFile(source_archive, mode="r") as archive:
            for member_name in archive.namelist():
                archive.extract(member_name, path=self.get_task_folder())
                self.log.info("'{}' was extracted.".format(member_name))

        return self.get_task_folder()


class GZipOperator(PipelineOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(GZipOperator, self).__init__(*args, **kwargs)

    def _execute_with_folder(self, context):
        task_instance: TaskInstance = context["task_instance"]
        folder: str = task_instance.xcom_pull("unzip_task")
        self.log.info("Folder retrieved from upstream task: {}".format(folder))

        for f in listdir(folder):
            file_path = join(folder, f)
            if isfile(file_path) and file_path.endswith(".gz"):
                continue

            with open(file_path, "rb") as plain_file:
                with gzip.open("{}.gz".format(join(self.get_task_folder(), f)), "wb") as gzip_file:
                    gzip_file.writelines(plain_file.readlines())

            self.log.debug("{} was gzipped.".format(file_path))
