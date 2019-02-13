from airflow.plugins_manager import AirflowPlugin
from .operators import ExtractURLOperator, DownloadOperator, FakeDownloadOperator, ChecksumOperator, UnzipOperator, \
    GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [ExtractURLOperator,
                 DownloadOperator,
                 FakeDownloadOperator,
                 ChecksumOperator,
                 UnzipOperator,
                 GZipOperator]
