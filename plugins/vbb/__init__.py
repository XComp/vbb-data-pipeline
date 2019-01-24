from airflow.plugins_manager import AirflowPlugin
from .operators import ExtractURLOperator, DownloadOperator, ChecksumOperator, UnzipOperator, GZipOperator


class VBBPlugin(AirflowPlugin):
    name = "vbb_plugin"
    operators = [ExtractURLOperator, DownloadOperator, ChecksumOperator, UnzipOperator, GZipOperator]
