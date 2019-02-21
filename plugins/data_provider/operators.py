import requests
import zipfile

from os import listdir, makedirs, remove
from os.path import join, exists
from shutil import copyfile
from typing import Callable
from urllib.parse import urlparse, ParseResult

from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults


class DataProviderOperator(BaseOperator):

    @apply_defaults
    def __init__(self, provider_id: str, base_folder: str, *args, **kwargs):
        super(DataProviderOperator, self).__init__(*args, **kwargs)

        self.provider_id: str = provider_id
        self.base_folder: str = base_folder

        provider_folder = self.get_provider_folder()
        if not exists(provider_folder):
            makedirs(provider_folder)
            self.log.info("Created '{}'...".format(provider_folder))

    def get_provider_id(self):
        return self.provider_id

    def get_base_folder(self):
        return self.base_folder

    def get_provider_folder(self):
        return join(self.get_base_folder(), self.get_provider_id())

    def execute(self, context):
        pass


class ExtractURLOperator(SkipMixin, DataProviderOperator):

    @apply_defaults
    def __init__(self, url: str, extract_download_url: Callable, check_url: bool, *args, **kwargs):
        super(ExtractURLOperator, self).__init__(*args, **kwargs)

        self.url: ParseResult = urlparse(url)
        self.extract_download_url = extract_download_url
        self.check_url = check_url

    def _get_url_filepath(self) -> str:
        return join(self.get_provider_folder(), "url.txt".format(self.get_provider_id()))

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

    def execute(self, context):
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


class DownloadOperator(DataProviderOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DownloadOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        response = requests.get(context["task_instance"].xcom_pull("extract_url_task"))

        target_file = join(self.get_provider_folder(), "new_archive.zip")
        with open(target_file, "wb") as zip_archive:
            zip_archive.write(response.content)

        self.log.info("Download finished.")

        return target_file


class FakeDownloadOperator(DataProviderOperator):

    @apply_defaults
    def __init__(self, source_file, *args, **kwargs):
        super(FakeDownloadOperator, self).__init__(*args, **kwargs)

        self.source_file = source_file

    def execute(self, context):
        target_file = join(self.get_provider_folder(), "new_archive.zip")
        copyfile(self.source_file, target_file)

        self.log.info("Copying finished.")

        return target_file


class ChecksumOperator(SkipMixin, DataProviderOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ChecksumOperator, self).__init__(*args, **kwargs)

    @staticmethod
    def _calculate_checksum(zip_archive_path: str):
        z = zipfile.ZipFile(zip_archive_path, "r")
        checksum = 0
        for info in z.infolist():
            checksum = checksum ^ info.CRC

        return checksum

    def execute(self, context):
        archive_path = context["task_instance"].xcom_pull("download_task")

        checksum: int = ChecksumOperator._calculate_checksum(zip_archive_path=archive_path)

        for old_archive in listdir(self.get_provider_folder()):
            old_archive_path = join(self.get_provider_folder(), old_archive)

            if old_archive.endswith(".zip") \
                    and not archive_path.endswith(old_archive) \
                    and checksum == ChecksumOperator._calculate_checksum(old_archive_path):
                # the file does already exist
                self.log.info("Archive didn't change: Deleting downloaded archive.")
                remove(archive_path)

                return

        new_archive_path = join(self.get_provider_folder(), "{}.zip".format(context["ds"]))
        copyfile(archive_path, new_archive_path)
        remove(archive_path)

        return new_archive_path
